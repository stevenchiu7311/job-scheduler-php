<?php
include 'vendor/autoload.php';
include __DIR__.'/../src/RedisFactory.php';

use TimeWorker\JobManager;
use TimeWorker\JobScheduler;
use React\EventLoop\Factory;

$redisConf['host'] = '127.0.0.1';
$redisConf['port'] = '6379';
$db = isset($redisConf['db']) ? $redisConf['db'] : 0;

const VALID_INTENTS = ['morning', 'bye', 'imhome', 'night'];

if ($argc > 1) {
    list($SELF, $isJobSubmitter) = $argv;
}

$looper = Factory::create();
if (!isset($isJobSubmitter)) {
    if (strpos($_SERVER['OS'], 'Windows') === false) {
        $process = new \React\ChildProcess\Process("php job_test.php true");
        $process->start($this->looper);
    } else {
        init($redisConf, isset($isJobSubmitter)? $isJobSubmitter: true);
    }
}
init($redisConf, isset($isJobSubmitter)? $isJobSubmitter: false);

function init($redisConf, $isJobSubmitter = false)
{
    global $db;
    try {
        $redisConnect = getRedisConn($redisConf);
        if ($isJobSubmitter == "true") {
            testAddJob($redisConnect);
        } else {
            $scheduler = new JobScheduler(0, $redisConf['host'], $redisConf['port'], $db, 180, 10);
            $scheduler->run(function ($key, $value, $way) {
                $wayStr = $way == 0 ? 'polling' : 'notification';
                echo "Job is ready: key: $key value: $value [$wayStr]\n";
            });
        }
    } catch (\RedisException $exception) {
        echo "Exception:$exception\n";
        echo "Prepare to quit...\n";
        return;
    } catch (wxception\CacheException $exception) {
        echo "Exception:$exception\n";
        echo "Prepare to quit...\n";
        return;
    }
}

function testAddJob($redis)
{
    echo "================AddJob================\n";
    $strJsonFileContents = file_get_contents(__DIR__.'/'."script.json");
    $array = json_decode($strJsonFileContents);
    $manager = new JobManager();
    foreach (VALID_INTENTS as $intent) {
        $commands = $array->$intent->commands;
        $timeDelay = 0;
        $commandNo = 0;
        foreach ($commands as $key => $command) {
            $timeDelay += isset($command->duration)? $command->duration : 1;
            $commandNo++;
            $manager->addJob($redis, json_encode($command), $timeDelay, "testId", $intent, 0, $commandNo);
        }
    }
    echo "======================================\n";
}

function testScheduler($redis)
{
    $strJsonFileContents = file_get_contents(__DIR__.'/'."script.json");
    $array = json_decode($strJsonFileContents);
    $manager = new JobManager();
    foreach (VALID_INTENTS as $intent) {
        $commands = $array->$intent->commands;
        $timeDelay = 0;
        $commandNo = 0;
        foreach ($commands as $key => $command) {
            $timeDelay += isset($command->duration)? $command->duration : 1;
            $commandNo++;
            $manager->addJob($redis, json_encode($command), $timeDelay, "testId", $intent, 0, $commandNo);
        }
    }
}
