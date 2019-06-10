<?php
include __DIR__.'/../vendor/autoload.php';

use TimeWorker\JobManager;
use TimeWorker\JobScheduler;
use TimeWorker\RedisFactory;
use React\EventLoop\Factory;

$redisConf['host'] = '127.0.0.1';
$redisConf['port'] = '6379';
$db = isset($redisConf['db']) ? $redisConf['db'] : 0;

const VALID_INTENTS = ['morning', 'bye', 'imhome', 'night'];

init($redisConf, isset($isJobSubmitter)? $isJobSubmitter: false);

function init($redisConf, $isJobSubmitter = false)
{
    global $db;
    try {
        $scheduler = new JobScheduler(0, $redisConf['host'], $redisConf['port'], $db, 180, 30, __DIR__."/../vendor/autoload.php");
        testAddJob($redisConf);
        $scheduler->getLooper()->addPeriodicTimer(20, function () use ($redisConf) {
            testAddJob($redisConf);
        });
        $scheduler->run(function ($attrs, $value, $way) {
            $wayStr = $way == 0 ? 'polling' : 'notification';
            echo "Job is ready: key: ".json_encode($attrs)." value: $value [$wayStr]\n";
        });
    } catch (\RedisException $exception) {
        echo "Exception:$exception\n";
        echo "Prepare to quit...\n";
        return;
    } catch (TimeWorker\CacheException $exception) {
        echo "Exception:$exception\n";
        echo "Prepare to quit...\n";
        return;
    }
}

function testAddJob($redisConf)
{
    $redis = TimeWorker\RedisFactory::getRedisConn($redisConf);
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
    $redis->close();
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
