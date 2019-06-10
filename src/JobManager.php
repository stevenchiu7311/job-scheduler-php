<?php

namespace TimeWorker;

class JobManager
{
    const KEY_DELIMETER  = ':';
    const JOB_KEY = 'timework-jobs';
    const SCHEDULE_KEY_PREFIX = 'timework-schedule'.self::KEY_DELIMETER;
    const HASH_TABLE_NAME = 'timework-contents';
    const CONTENT_KEY_PREFIX = 'timework-contents'.self::KEY_DELIMETER;

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

        // Save content in specified redis key and set timer for execution
        $expiredKey = self::SCHEDULE_KEY_PREFIX.$keySegment;
        $expiredValue = self::CONTENT_KEY_PREFIX.$keySegment;
        $redis->pSetEx($expiredKey, $delay * 1000 + 1, $expiredValue);

        $key = self::CONTENT_KEY_PREFIX.$keySegment;
        $redis->hset(self::HASH_TABLE_NAME, $key, $value);
        echo "Add key:$key delay: $delay\n";
    }

    public function removeJob($redis, ...$attr)
    {
        $keySegment = join(':', $attr);
        $key = self::SCHEDULE_KEY_PREFIX.$keySegment;
        $this->removeJobByScheduleKey($redis, $key);
    }

    public function removeJobByScheduleKey($redis, $scheduleKey)
    {
        $contentkey = preg_replace('/^'.self::SCHEDULE_KEY_PREFIX.'/', self::CONTENT_KEY_PREFIX, $scheduleKey);
        $redis->zrem(self::JOB_KEY, $scheduleKey);
        $redis->delete($scheduleKey);
        $content = $redis->hget(self::HASH_TABLE_NAME, $contentkey);
        $redis->hdel(self::HASH_TABLE_NAME, $contentkey);
        echo 'Remove content via key '.$contentkey."\n";

        return $content;
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
                    $content = $this->removeJobByScheduleKey($redis, $key);
                    $retKey = preg_replace('/^'.self::SCHEDULE_KEY_PREFIX.'/', '', $key);
                    $cb(explode(self::KEY_DELIMETER, $retKey), $content, 0);
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
