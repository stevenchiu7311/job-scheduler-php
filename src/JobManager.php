<?php

namespace TimeWorker;

class JobManager
{
    const JOB_KEY = 'ilife:jobs';
    const SCHEDULE_KEY_PREFIX = 'ilife:schedule:';
    const HASH_TABLE_NAME = 'ilife:commands';
    const COMMAD_KEY_PREFIX = 'ilife:commands:';

    const DEFAULT_POLLING_INTERVAL = 60;

    private $looper;
    private $pollingInterval;

    public function __construct($looper = null, $pollingInterval = self::DEFAULT_POLLING_INTERVAL)
    {
        $this->looper = $looper;
        $this->pollingInterval = $pollingInterval;
    }

    public function addJob($redis, $value, $delay = 1, ...$attr)
    {
        $keySegment = join(':', $attr);

        $ts = time();

        // Add job into redis sorted set
        $due = $ts + $delay;
        $setItemKey = self::SCHEDULE_KEY_PREFIX.$keySegment;
        $redis->zAdd(self::JOB_KEY, $due, $setItemKey);

        // Save command in specified redis key and set timer for execution
        $expiredKey = self::SCHEDULE_KEY_PREFIX.$keySegment;
        $expiredValue = self::COMMAD_KEY_PREFIX.$keySegment;
        $redis->pSetEx($expiredKey, $delay * 1000 + 1, $expiredValue);

        $key = self::COMMAD_KEY_PREFIX.$keySegment;
        $redis->hset(self::HASH_TABLE_NAME, $key, $value);
        echo "Add key:$key delay: $delay\n";
    }

    public function removeJob($redis, $deviceId = 'testId', $intent = 'good_morning', $seq = 0, $commandNo = 0)
    {
        $keySegment = join(':', $attr);
        $key = self::SCHEDULE_KEY_PREFIX.$keySegment;
        $this->removeJobByScheduleKey($redis, $key);
    }

    public function removeJobByScheduleKey($redis, $scheduleKey)
    {
        $commandkey = preg_replace('/^'.self::SCHEDULE_KEY_PREFIX.'/', self::COMMAD_KEY_PREFIX, $scheduleKey);
        $redis->zrem(self::JOB_KEY, $scheduleKey);
        $redis->delete($scheduleKey);
        $command = $redis->hget(self::HASH_TABLE_NAME, $commandkey);
        $redis->hdel(self::HASH_TABLE_NAME, $commandkey);
        echo 'Remove command key '.$commandkey."\n";

        return $command;
    }

    public function run($redis, callable $cb)
    {
        $this->check($redis, $cb);
        $timer = $this->looper->addPeriodicTimer($this->pollingInterval, function () use (&$redis, $cb) {
            $this->check($redis, $cb);
        });

        $this->looper->run();
    }

    private function check(&$redis, callable $cb)
    {
        $ts = time();
        try {
            $candidates = $redis->zRangeByScore(self::JOB_KEY, 0, $ts, array('withscores' => true));
            if ($candidates) {
                foreach ($candidates as $key => $candidate) {
                    $command = $this->removeJobByScheduleKey($redis, $key);
                    $cb($key, $command, 0);
                }
            }
            if (sizeof($candidates) > 0) {
                print_r($candidates);
            }
        } catch (\RedisException $exception) {
            echo "Exception:$exception\n";
            echo "Quit looper...\n";
            $this->looper->stop();
        }
    }
}
