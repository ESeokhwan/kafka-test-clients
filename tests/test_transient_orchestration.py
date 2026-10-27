"""Isolated orchestration tests: no Kafka, SSH, Gradle, Java, fio, TRIM or cache writes."""
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash")
TRANSIENT_CLASSES = [
    "org/example/apps/AutoTransientTopicProducerMetrics.class",
    "org/apache/kafka/clients/producer/KafkaTransientProducer.class",
    "org/apache/kafka/clients/producer/TransientProducerRecord.class",
    "org/apache/kafka/clients/producer/TransientTopicHandle.class",
    "org/apache/kafka/clients/producer/TransientCallback.class",
]
RESET_CLASSES = [
    "org/example/apps/ResetTransientTopics.class",
    "org/apache/kafka/clients/admin/ResetTransientTopicsOptions.class",
    "org/apache/kafka/clients/admin/ResetTransientTopicsResult.class",
]

# Every command with side effects is replaced in an isolated project copy.
MOCK = r'''
import json
import os
from pathlib import Path
import sys
import time

args = sys.argv[1:]
trace = Path(os.environ["MOCK_TRACE"])

def record(kind, **details):
    with trace.open("a") as handle:
        handle.write(json.dumps(dict(kind=kind, args=args, **details)) + "\n")

def reset(reset_class=None):
    record("reset", pid=os.getpid(), reset_class=reset_class)
    time.sleep(float(os.environ.get("MOCK_RESET_DELAY", "0")))
    sys.exit(int(os.environ.get("MOCK_RESET_STATUS", "0")))

if program == "setsid":
    os.setsid()
    os.execvp(args[0], args)
elif program == "sleep":
    time.sleep(min(float(args[0]), .02))
elif program == "uname":
    print("Linux")
elif program == "sudo":
    assert args.pop(0) == "-n"
    if args != ["true"]:
        os.execvp(args[0], args)
elif program == "sh":
    assert args == ["-c", 'sync && printf "3\\n" > /proc/sys/vm/drop_caches']
    record("cache")
elif program == "javac":
    record("javac")
elif program == "java":
    if "JmxPreflight" in args:
        record("jmx")
        print("[JMX Preflight] READY")
    elif os.environ.get("CLEANUP_ONCE") == "true":
        record("cleanup", prefix=os.environ["CLEANUP_TOPIC_PREFIX"],
               start=os.environ["CLEANUP_START_INDEX"],
               count=os.environ["CLEANUP_TOPIC_COUNT"])
    else:
        raise AssertionError("Unexpected Java invocation")
elif program == "runner":
    assert args[0] == "apps"
    producer_class = args[1]
    if producer_class == "BrokerExtension":
        broker = args[args.index("--broker") + 1]
        record("rollout", broker=broker)
        print("MOCK_ROLLOUT_OUTPUT broker=" + broker)
        status = int(os.environ.get("MOCK_ROLLOUT_STATUS", "0"))
        if os.environ.get("MOCK_ROLLOUT_FAIL_BROKER") == broker:
            status = 7
        if status:
            print("MOCK_ROLLOUT_ERROR exit_code=" + str(status), file=sys.stderr)
        sys.exit(status)
    if producer_class == "ResetTransientTopics":
        args = args[2:]
        reset(producer_class)
    if producer_class == "AutoTransientTopicProducerMetrics":
        assert not any(arg.split("=")[0] in (
            "--check-topics-absent", "--prepare-topics-only",
            "--require-existing-topics", "--ignore-response") for arg in args)
    if "--check-topics-absent" in args:
        record("check", producer_class=producer_class)
    elif "--prepare-topics-only" in args:
        record("precreate", producer_class=producer_class)
    else:
        record("producer", producer_class=producer_class)
        if os.environ.get("MOCK_WAIT_IOSTAT"):
            deadline = time.monotonic() + 5
            while '"kind": "iostat"' not in trace.read_text():
                if time.monotonic() >= deadline:
                    raise AssertionError("iostat did not start")
                time.sleep(.01)
        if os.environ.get("MOCK_INTERRUPT_PRODUCER"):
            import signal
            os.kill(os.getppid(), signal.SIGTERM)
            time.sleep(5)
        marker = trace.with_suffix(".producer-failed")
        if os.environ.get("MOCK_FAIL_PRODUCER_ONCE") and not marker.exists():
            marker.touch()
            sys.exit(17)
        if "--stats-disabled" not in args:
            metrics = Path(args[args.index("--metrics-file") + 1])
            metrics.write_text("TestCreator,Sample Count\nALL,2\n")
        if "--full-file" in args:
            full = Path(args[args.index("--full-file") + 1])
            full.write_text("Content,Status,RequestedAt,RespondedAt,Latency\nmessage,SUCCESS,1,2,1\n")
elif program == "reset":
    reset()
elif program in ("fio", "trim", "build"):
    record(program)
    if program == "fio" and os.environ.get("MOCK_FAIL_FIO"):
        sys.exit(9)
elif program == "iostat":
    record("iostat")
    print("MOCK_IOSTAT", flush=True)
    while True:
        time.sleep(.05)
elif program == "remote":
    index = 0
    while args[index].startswith("--"):
        index += 2
    operation = args[index]
    assert operation in ("check", "fio", "cache", "trim", "iostat")
    record("remote_" + operation)
elif program == "ssh":
    raise AssertionError("The real SSH transport must not be invoked")
else:
    raise AssertionError("Unexpected mock command: " + program)
'''


class TransientOrchestrationTests(unittest.TestCase):
    topic_mode = "transient"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="transient-orchestration-")
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)
        self.project = self.work / "project"
        self.project.mkdir()
        shutil.copyfile(ROOT / "run_kafka_experiments.sh",
                        self.project / "run_kafka_experiments.sh")
        tools = self.project / "tools"
        tools.mkdir()
        for name in ("kst_console.py", "finalize_experiment.py"):
            shutil.copyfile(ROOT / "tools" / name, tools / name)
        (tools / "JmxPreflight.java").write_text("// mock compiler input\n")
        (tools / "remote_broker.py").write_text('program = "remote"\n' + MOCK)
        (self.project / "KafkaJmxProducerBatchDaemon.java").write_text(
            "// DELETE_TOPIC_PREFIX CLEANUP_ONCE\n")
        (self.project / "settings.gradle").write_text("// mock build\n")
        self.bin = self.work / "bin"
        self.bin.mkdir()
        for name in ("setsid", "sleep", "uname", "sudo", "sh", "javac", "java",
                     "runner", "reset", "fio", "trim", "build", "iostat", "ssh"):
            command = self.bin / name
            command.write_text(f"#!{sys.executable}\nprogram = {name!r}\n" + MOCK)
            command.chmod(0o755)
        scripts = self.project / "scripts"
        scripts.mkdir()
        self.write_wrapper(scripts / "kafka_clients_run.sh", "runner")
        self.write_wrapper(self.project / "gradlew", "build")
        self.write_wrapper(self.project / "prepare_ssd_fio.sh", "fio")
        self.write_wrapper(self.project / "prepare_ssd_trim.sh", "trim")
        self.reset_script = self.work / "reset command.sh"
        self.write_wrapper(self.reset_script, "reset")
        self.jar = self.project / "build/libs/apps-1.0-SNAPSHOT-all.jar"
        self.jar.parent.mkdir(parents=True)
        self.write_jar(TRANSIENT_CLASSES + RESET_CLASSES)
        self.output = self.work / "output"
        self.trace = self.work / "trace.jsonl"
        self.env = dict(os.environ)
        for name in (
            "TOPIC_MODE", "CLEANUP_ENABLED", "DELETE_ENABLED", "TRANSIENT_RESET_SCRIPT",
            "BROKER_SSH", "BROKER_PROJECT_DIR", "BUILD_ENABLED", "RETRY", "PREFIX",
            "CLEANUP_TOPIC_PREFIX", "DELETE_TOPIC_PREFIX", "EXPERIMENT_CLASS_DIR",
            "MONITOR_ROLLOUT_TIMEOUT_MS", "MONITOR_ROLLOUT_COMMAND_CONFIG",
        ):
            self.env.pop(name, None)
        self.env.update(
            PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
            MOCK_TRACE=str(self.trace),
            CLIENT_DIR=str(self.project),
            RUNNER=str(scripts / "kafka_clients_run.sh"),
            FIO_SCRIPT=str(self.project / "prepare_ssd_fio.sh"),
            DAEMON_SOURCE=str(self.project / "KafkaJmxProducerBatchDaemon.java"),
        )

    def write_wrapper(self, path, command):
        path.write_text(
            "#!/usr/bin/env bash\nexec "
            + shlex.quote(str(self.bin / command)) + ' "$@"\n')

    def write_jar(self, entries):
        # Only archive membership is inspected; no bytecode is executed.
        with zipfile.ZipFile(self.jar, "w") as jar:
            for entry in entries:
                jar.writestr(entry, b"mock")

    def run_case(self, *args, env=None, selection=("--hz", "2")):
        command = [
            BASH, str(self.project / "run_kafka_experiments.sh"),
            *selection, "--no-build", "--no-fio", "--no-fstrim",
            "--no-drop-caches", "--no-iostat", "--step-delay-seconds", "0",
            "--inter-experiment-delay-seconds", "0", "--topic-cnt", "2",
            "--run-duration-minutes", "0.01", "--output-dir", str(self.output),
            *args,
        ]
        result = subprocess.run(command, env=dict(self.env, **(env or {})),
                                capture_output=True, text=True, timeout=20)
        return result

    def calls(self, kind=None):
        calls = [json.loads(line) for line in self.trace.read_text().splitlines()] \
            if self.trace.exists() else []
        return [call for call in calls if kind is None or call["kind"] == kind]

    def metadata(self):
        return "\n".join(path.read_text() for path in self.output.rglob("*.run.txt"))

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_transient_selects_class_without_normal_topic_operations(self):
        self.assert_success(self.run_case("--topic-mode", self.topic_mode, "--no-cleanup",
                                          "--need-flush", "--share-producer"))
        producer, = self.calls("producer")
        self.assertEqual(producer["producer_class"], "AutoTransientTopicProducerMetrics")
        self.assertIn("--need-flush", producer["args"])
        self.assertIn("--share-producer", producer["args"])
        if self.topic_mode == "transient-precreate":
            self.assertIn("--precreate", producer["args"])
            self.assertEqual(producer["args"][producer["args"].index("--topic-prepare-timeout-seconds") + 1], "300")
        else:
            self.assertNotIn("--precreate", producer["args"])
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "rollout"])
        metadata = self.metadata()
        for expected in (f"TOPIC_MODE={self.topic_mode}", "CLEANUP_POLICY=none",
                         "PRODUCER_CLASS=AutoTransientTopicProducerMetrics",
                         "status=SKIPPED reason=transient", "status=SUCCESS",
                         "stage=COOLDOWN_END"):
            self.assertIn(expected, metadata)
        self.assertEqual(len(list(self.output.glob(f"*_{self.topic_mode}_*_metrics.csv"))), 1)
        self.assertFalse(list(self.output.glob("*_end_PENDING*")))
        index, = self.output.glob("*_runs.tsv")
        self.assertNotIn("_end_PENDING", index.read_text())

    def test_auto_keeps_existing_check_producer_and_bounded_cleanup(self):
        self.assert_success(self.run_case("--topic-mode", "auto", "--start-index", "10"))
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["javac", "check", "producer", "cleanup", "rollout"])
        producer, = self.calls("producer")
        self.assertEqual(producer["producer_class"], "AutoTopicProducerMetrics")
        cleanup, = self.calls("cleanup")
        self.assertEqual((cleanup["prefix"], cleanup["start"], cleanup["count"]),
                         ("test_creator_0_", "10", "2"))

    def test_precreate_keeps_existing_order_and_producer_flags(self):
        self.assert_success(self.run_case("--topic-mode", "precreate", "--fio"))
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["javac", "cleanup", "check", "precreate", "fio", "producer", "cleanup", "rollout"])
        self.assertIn("--require-existing-topics", self.calls("producer")[0]["args"])

    def test_default_cleanup_uses_reset_app_without_external_script(self):
        result = self.run_case("--topic-mode", self.topic_mode, "--build", "--fio")
        self.assert_success(result)
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["build", "fio", "producer", "reset", "rollout"])
        reset, = self.calls("reset")
        self.assertEqual(reset["reset_class"], "ResetTransientTopics")
        self.assertEqual(reset["args"],
                         ["--bootstrap-server", "166.104.110.11:9092", "--timeout-ms", "300000"])
        self.assertIn("apps ResetTransientTopics", self.metadata())
        self.assertIn("stage=TRANSIENT_RESET_SUCCESS", self.metadata())

    def test_invalid_transient_combinations_are_rejected_before_build(self):
        cases = [
            ["--delete"],
            ["--ignore-response"],
            ["--", "--ignore-response"],
            ["--", "--require-existing-topics"],
            ["--", "--prepare-topics-only"],
            ["--", "--check-topics-absent"],
            ["--mode", "cleanup"],
        ]
        for extra in cases:
            with self.subTest(extra=extra):
                result = self.run_case("--topic-mode", self.topic_mode, "--no-cleanup",
                                       "--build", *extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.calls(), [])

    def test_normal_mode_still_requires_cleanup(self):
        result = self.run_case("--topic-mode", "auto", "--no-cleanup")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_transient_reset_order_arguments_and_repeated_rounds(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2", "--fio", "--drop-caches",
            "--brokers", "broker-a:9092,broker-b:9092", "--cleanup",
            "--transient-reset-script", str(self.reset_script),
            "--cleanup-timeout-seconds", "7"))
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["cache", "fio", "producer", "reset", "cache", "rollout", "rollout"] * 2)
        for reset in self.calls("reset"):
            self.assertEqual(reset["args"], [
                "--bootstrap-server", "broker-a:9092,broker-b:9092", "--timeout-ms", "7000"])
        self.assertIn("CLEANUP_POLICY=reset-transient-topics", self.metadata())
        self.assertEqual(self.metadata().count("stage=TRANSIENT_RESET_SUCCESS"), 2)
        self.assertEqual(len(list(self.output.glob("*_reset.log"))), 2)
        self.assertEqual(self.calls("javac"), [])

    def test_reset_failure_is_not_retried_and_stops_next_round(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2",
            env={"MOCK_RESET_STATUS": "8"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "reset", "rollout"])
        self.assertNotIn("status=FAILED_CLEANED", self.metadata())
        self.assertNotIn("stage=TRANSIENT_RESET_SUCCESS", self.metadata())
        self.assertIn("next_experiment_allowed=false", self.metadata())

    def test_reset_timeout_stops_child_and_next_round_without_retry(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2",
            "--cleanup-timeout-seconds", "1",
            env={"MOCK_RESET_DELAY": "5"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.calls("reset")), 1)
        self.assertEqual(len(self.calls("producer")), 1)
        self.assertIn("작업 시간 초과", self.metadata())
        with self.assertRaises(ProcessLookupError):
            os.kill(self.calls("reset")[0]["pid"], 0)

    def test_failed_recovery_reset_aborts_remaining_rounds(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2",
            env={"MOCK_FAIL_PRODUCER_ONCE": "1", "MOCK_RESET_STATUS": "9"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "reset", "rollout"])
        self.assertIn("status=RECOVERY_FAILED", self.metadata())
        self.assertNotIn("status=FAILED_CLEANED", self.metadata())

    def test_failed_producer_resets_before_next_round_and_reports_failure(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2",
            env={"MOCK_FAIL_PRODUCER_ONCE": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["producer", "reset", "rollout", "producer", "reset", "rollout"])
        self.assertIn("status=FAILED_CLEANED", self.metadata())
        self.assertIn("status=SUCCESS", self.metadata())

    def test_failed_producer_without_cleanup_stops_without_reset(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--no-cleanup", "--retry", "2",
            "--transient-reset-script", str(self.reset_script),
            env={"MOCK_FAIL_PRODUCER_ONCE": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "rollout"])
        self.assertNotIn("status=FAILED_CLEANED", self.metadata())
        self.assertIn("status=RECOVERY_SKIPPED", self.metadata())

    def test_failed_preparation_does_not_reset_cluster(self):
        result = self.run_case(
            "--topic-mode", self.topic_mode, "--fio", "--retry", "2",
            env={"MOCK_FAIL_FIO": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()], ["fio", "rollout"])
        self.assertIn("status=PREFLIGHT_FAILED", self.metadata())

    def test_stale_or_invalid_jar_is_rejected_before_fio(self):
        for entries in [[], TRANSIENT_CLASSES[:-1]]:
            with self.subTest(entries=entries):
                self.write_jar(entries)
                result = self.run_case("--topic-mode", self.topic_mode, "--no-cleanup", "--fio")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("커스텀 Kafka API", result.stdout)
                self.assertEqual(self.calls(), [])
        self.jar.write_text("invalid archive")
        result = self.run_case("--topic-mode", self.topic_mode, "--no-cleanup", "--fio")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_remote_mode_keeps_transient_production_on_client(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--no-cleanup", "--fio",
            "--broker-ssh", "test@mock-broker", "--broker-project-dir", "/mock"))
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["remote_check", "remote_fio", "producer", "rollout"])
        self.assertEqual(self.calls("producer")[0]["producer_class"],
                         "AutoTransientTopicProducerMetrics")

    def test_remote_cleanup_runs_reset_app_on_client(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--fio",
            "--broker-ssh", "test@mock-broker", "--broker-project-dir", "/mock"))
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["remote_check", "remote_fio", "producer", "reset", "rollout"])
        self.assertEqual(self.calls("reset")[0]["reset_class"], "ResetTransientTopics")

    def test_builtin_cleanup_rejects_jar_missing_reset_app_or_api(self):
        for entries in [TRANSIENT_CLASSES, TRANSIENT_CLASSES + RESET_CLASSES[:-1]]:
            with self.subTest(entries=entries):
                self.write_jar(entries)
                result = self.run_case("--topic-mode", self.topic_mode, "--fio")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("JAR에 필요한 클래스가 없습니다", result.stdout)
                self.assertEqual(self.calls(), [])

    def test_no_cleanup_and_external_override_allow_jar_without_reset_app(self):
        self.write_jar(TRANSIENT_CLASSES)
        self.assert_success(self.run_case("--topic-mode", self.topic_mode, "--no-cleanup"))
        self.assertEqual(self.calls("reset"), [])
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--cleanup",
            "--transient-reset-script", str(self.reset_script)))
        self.assertIsNone(self.calls("reset")[0]["reset_class"])

    def test_build_once_and_cli_overrides_environment_mode(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--no-cleanup", "--build", "--retry", "2",
            env={"TOPIC_MODE": "auto"}))
        self.assertEqual(len(self.calls("build")), 1)
        self.assertEqual(len(self.calls("producer")), 2)
        self.assertEqual(self.calls("check"), [])

    def test_stats_disabled_does_not_require_csv(self):
        self.assert_success(self.run_case(
            f"--topic-mode={self.topic_mode}", "--no-cleanup", "--stats-disabled"))
        self.assertFalse(list(self.output.rglob("*_metrics.csv")))
        self.assertIn("status=SUCCESS", self.metadata())

    def test_transient_iostat_is_collected_and_finalized(self):
        # Keep the mock experiment alive for one collection interval before stopping iostat.
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--no-cleanup", "--iostat",
            "--inter-experiment-delay-seconds", "1", env={"MOCK_WAIT_IOSTAT": "1"}))
        self.assertCountEqual([call["kind"] for call in self.calls()], ["iostat", "producer", "rollout"])
        log, = self.output.glob("*_iostat.log")
        self.assertIn("MOCK_IOSTAT", log.read_text())
        self.assertIn("IOSTAT_ENABLED=true", self.metadata())

    def test_last_transient_round_without_cleanup_still_observes_delay(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--no-cleanup",
            "--inter-experiment-delay-seconds", "1"))
        event, = [line for line in self.metadata().splitlines() if "stage=COOLDOWN_END" in line]
        self.assertGreaterEqual(int(event.split("elapsed_seconds=")[1]), 1)

    def test_normal_failure_still_cleans_and_continues(self):
        result = self.run_case("--topic-mode", "auto", "--retry", "2",
                               env={"MOCK_FAIL_PRODUCER_ONCE": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()],
                         ["javac", "check", "producer", "cleanup", "rollout",
                          "check", "producer", "cleanup", "rollout"])
        self.assertIn("status=FAILED_CLEANED", self.metadata())
        self.assertIn("status=SUCCESS", self.metadata())

    def test_rollout_runs_once_for_every_hz_and_repeat_including_the_last(self):
        self.assert_success(self.run_case("--topic-mode", self.topic_mode, "--retry", "2", selection=("--all",)))
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "reset", "rollout"] * 10)
        records = list(self.output.rglob("*.run.txt"))
        self.assertEqual(len(records), 10)
        for record in records:
            text = record.read_text()
            self.assertEqual(text.count("stage=MONITOR_ROLLOUT_SUCCESS"), 1)
            self.assertIn("MOCK_ROLLOUT_OUTPUT broker=166.104.110.11:9092", text)
            self.assertIn("FINAL_EXIT_CODE=0", text)
            self.assertLess(text.index("stage=COOLDOWN_END"), text.index("stage=MONITOR_ROLLOUT_START"))
            self.assertLess(text.index("stage=MONITOR_ROLLOUT_SUCCESS"), text.index("FINAL_EXIT_CODE=0"))
            self.assertNotIn("_end_PENDING", record.name)

    def test_rollout_failure_does_not_stop_other_brokers_or_repeated_experiments(self):
        config = self.work / "monitor security.properties"
        config.write_text("security.protocol=SASL_SSL\n")
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2", "--output-layout", "nested",
            "--brokers", " broker-a:9092 , [::1]:9092 ", "--monitor-rollout-timeout-ms", "1500",
            "--monitor-rollout-command-config", str(config),
            env={"MOCK_ROLLOUT_FAIL_BROKER": "broker-a:9092"}))
        rollouts = self.calls("rollout")
        self.assertEqual([call["broker"] for call in rollouts], ["broker-a:9092", "[::1]:9092"] * 2)
        for call in rollouts:
            self.assertEqual(call["args"], ["apps", "BrokerExtension", "--broker", call["broker"],
                             "--target", "monitor-log-rollout", "--operation", "flush-and-rollout",
                             "--timeout-ms", "1500", "--command-config", str(config)])
        records = list(self.output.rglob("*.run.txt"))
        self.assertEqual(len(records), 2)
        for record in records:
            text = record.read_text()
            self.assertIn("stage=MONITOR_ROLLOUT_FAILED broker=broker-a:9092 exit_code=7 continue=true", text)
            self.assertIn("MOCK_ROLLOUT_ERROR exit_code=7", text)
            self.assertIn("stage=MONITOR_ROLLOUT_SUCCESS broker=[::1]:9092 exit_code=0", text)
            self.assertIn("FINAL_EXIT_CODE=0", text)
        self.assertFalse(list(self.output.rglob("*_failure.log")))

    def test_rollout_timeout_exit_code_is_logged_without_retry_or_failed_experiment(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--retry", "2", "--no-cleanup",
            env={"MOCK_ROLLOUT_STATUS": "124"}))
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "rollout"] * 2)
        self.assertEqual(self.metadata().count("exit_code=124 continue=true"), 2)
        self.assertNotIn("status=FAILED_CLEANED", self.metadata())

    def test_rollout_failure_preserves_failed_experiment_recovery_and_exit_status(self):
        result = self.run_case("--topic-mode", self.topic_mode, "--retry", "2",
                               env={"MOCK_FAIL_PRODUCER_ONCE": "1", "MOCK_ROLLOUT_STATUS": "1"})
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "reset", "rollout"] * 2)
        failed, = self.output.rglob("*repeat1*.run.txt")
        succeeded, = self.output.rglob("*repeat2*.run.txt")
        self.assertIn("FINAL_EXIT_CODE=20", failed.read_text())
        self.assertIn("FINAL_EXIT_CODE=0", succeeded.read_text())
        self.assertIn("status=FAILED_CLEANED next_experiment_allowed=true", failed.read_text())
        self.assertEqual(self.metadata().count("stage=MONITOR_ROLLOUT_FAILED"), 2)

    def test_standalone_cleanup_does_not_rollout(self):
        self.assert_success(self.run_case("--cleanup-topic-prefix", "test_scope_", selection=("--mode", "cleanup")))
        self.assertEqual([call["kind"] for call in self.calls()], ["javac", "cleanup"])
        self.assertNotIn("stage=MONITOR_ROLLOUT", self.metadata())

    def test_interruption_keeps_original_status_even_if_rollout_fails(self):
        result = self.run_case("--topic-mode", self.topic_mode, "--retry", "2",
                               env={"MOCK_INTERRUPT_PRODUCER": "1", "MOCK_ROLLOUT_STATUS": "1"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "rollout"])
        self.assertIn("FINAL_EXIT_CODE=143", self.metadata())
        self.assertIn("stage=MONITOR_ROLLOUT_FAILED", self.metadata())

    def test_invalid_rollout_timeout_is_rejected_before_work(self):
        for value in ("0", "-1", "abc", "2147483648"):
            with self.subTest(value=value):
                result = self.run_case("--monitor-rollout-timeout-ms", value)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.calls(), [])

    def test_missing_reset_script_and_invalid_timeout_are_rejected_early(self):
        for extra in [
            ["--transient-reset-script", str(self.work / "missing")],
            ["--transient-reset-script", str(self.reset_script),
             "--cleanup-timeout-seconds", "0"],
            ["--transient-reset-script", str(self.reset_script),
             "--cleanup-timeout-seconds", "86401"],
        ]:
            with self.subTest(extra=extra):
                result = self.run_case("--topic-mode", self.topic_mode, "--build", *extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.calls(), [])


class TransientPrecreateOrchestrationTests(TransientOrchestrationTests):
    # Both Transient modes must share validation, JAR checks, local/remote routing and failure recovery.
    topic_mode = "transient-precreate"

    def test_precreate_timeout_and_full_output_are_configured_and_recorded(self):
        self.assert_success(self.run_case(
            "--topic-mode", self.topic_mode, "--topic-prepare-timeout-seconds", "17"))
        self.assertEqual([call["kind"] for call in self.calls()], ["producer", "reset", "rollout"])
        args = self.calls("producer")[0]["args"]
        self.assertEqual(args.count("--precreate"), 1)
        self.assertEqual(args[args.index("--topic-prepare-timeout-seconds") + 1], "17")
        full, = self.output.glob("*_transient-precreate_*_full.csv")
        self.assertIn("SUCCESS", full.read_text())
        self.assertNotIn("_end_PENDING", full.name)
        self.assertIn("TOPIC_MODE=transient-precreate", self.metadata())
        self.assertIn("TOPIC_PREPARE_TIMEOUT_SECONDS=17", self.metadata())
        self.assertIn("PRECREATE_SEED_MESSAGES_PER_TOPIC=0", self.metadata())
        self.assertIn("CLEANUP_POLICY=reset-transient-topics", self.metadata())

    def test_precreate_overrides_are_rejected_before_build(self):
        for extra in [
            ["--precreate=false"], ["--precreate"],
            ["--topic-prepare-timeout-seconds", "1"],
            ["--topic-prepare-timeout-seconds=1"],
        ]:
            with self.subTest(extra=extra):
                result = self.run_case("--topic-mode", self.topic_mode, "--build", "--", *extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("실행기가 관리합니다", result.stdout)
                self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
