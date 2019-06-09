<?php
namespace TimeWorker;

include __DIR__.'/../vendor/autoload.php';

use React\EventLoop\Factory;

final class SchedulerTest extends \PHPUnit\Framework\TestCase
{
    const DEBUG = true;
    public function testScheduler()
    {
        $redisConf['host'] = '127.0.0.1';
        $redisConf['port'] = '6379';
        $db = isset($redisConf['db']) ? $redisConf['db'] : 0;
        $redis = RedisFactory::getRedisConn($redisConf);
        $manager = new JobManager();
        try {
            $result = [];
            for ($i = 2; $i < 10; $i++) {
                $result["SchedulerTest:$i"] = $i;
            }
            $scheduler = new JobScheduler(0, $redisConf['host'], $redisConf['port'], $db, 15, 15, __DIR__."/../vendor/autoload.php");
            self::addJob($redisConf, $result);
            $scheduler->run(function ($key, $value, $way) use ($result) {
                $wayStr = $way == 0 ? 'polling' : 'notification';
                $id = str_replace(JobManager::SCHEDULE_KEY_PREFIX, '', $key);
                $diff = time() - $result[$id];
                if (self::DEBUG) {
                    echo "current: ".time()." expected: $result[$id] Diff time: $diff\n";
                }
                $this->assertTrue($diff <= 3);
            });
        } catch (TimeWorker\CacheException $exception) {
            echo "Exception:$exception\n";
            echo "Prepare to quit...\n";
            return;
        } catch (TimeWorker\CacheException $exception) {
            echo "Exception:$exception\n";
            echo "Prepare to quit...\n";
            return;
        }
    }

    public function addJob($redisConf, &$result)
    {
        $redis = RedisFactory::getRedisConn($redisConf);
        $manager = new JobManager();
        foreach ($result as $id => $delay) {
            $manager->addJob($redis, "Content of $id", $delay, $id);
            $result[$id] = time() + $delay;
        }

        $redis->close();
    }
}
