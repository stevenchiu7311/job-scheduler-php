<?php

namespace TimeWorker;

use React\EventLoop\Factory;

class JobScheduler
{
    const EXECUTABLE_TIME = 1800; // 1800s
    const POLLING_INTERVAL = 60; // 60s

    const BUNDLE_KEY_PID = 'PID';
    const BUNDLE_KEY_STATUS = 'Status';
    const BUNDLE_KEY_PATTERN = 'Pattern';
    const BUNDLE_KEY_CHANNEL = 'Channel';
    const BUNDLE_KEY_PAYLOAD = 'Payload';

    private $forkChildCmd;
    private $executableTime;
    private $pollingInterval;
    private $subPattern;
    private $redisConf = [];
    private $cb;
    private $looper;

    public static function fork($api, $host, $port, $db)
    {
        $scheduler = new JobScheduler($api, $host, $port, $db);
        $scheduler->run();
    }

    public function __construct(
        $apiIndex = 0,
        $host = '127.0.0.1',
        $port = 6379,
        $db = 0,
        $executableTime = self::EXECUTABLE_TIME,
        $pollingInterval = self::POLLING_INTERVAL,
        $autoloadPath = __DIR__."/../../../../vendor/autoload.php"
    ) {
        $this->redisConf['host'] = $host;
        $this->redisConf['port'] = $port;
        $this->redisConf['db'] = $db;

        $this->apiIndex = $apiIndex;
        $this->subPattern = '__keyspace@' . $db . '__:';
        $this->executableTime = $executableTime;
        $this->pollingInterval = $pollingInterval;
        $this->forkChildCmd = "php -r \"include_once '$autoloadPath'; use TimeWorker\JobScheduler; TimeWorker\JobScheduler::fork(1, '$host', $port, $db);\"";

        $looper = Factory::create();
        $this->looper = $looper;
    }

    public function getLooper()
    {
        return $this->looper;
    }

    public function run(callable $cb = null)
    {
        $this->cb = $cb;

        try {
            $redisConnect = RedisFactory::getRedisConn($this->redisConf);
        } catch (\RedisException $exception) {
            echo "Exception:$exception\n";
            echo "Prepare to quit...\n";

            return;
        } catch (CacheException $exception) {
            echo "Exception:$exception\n";
            echo "Prepare to quit...\n";

            return;
        }

        if ($this->apiIndex == 0) {
            $this->performJobScheduler($redisConnect, $this->looper);
        } elseif ($this->apiIndex == 1) {
            $this->monitorExpireKey($this->redisConf, $this->looper);
        } else {
            throw new Exception('Invalid api index');
        }
    }

    public function runProcess($cmd)
    {
        $process = new \React\ChildProcess\Process($cmd);
        if (!isset($_SERVER['OS']) || strpos($_SERVER['OS'], 'Windows') === false) {
            $process->start($this->looper);
        }
    }

    public function keyEventMonitor($redis)
    {
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, -1);
        $redis->psubscribe(array($this->subPattern . JobManager::SCHEDULE_KEY_PREFIX . '*'), [$this, 'eventCallback']);
    }

    public function eventCallback($redis, $pattern, $chan, $msg)
    {
        // Notice: Should watch "expired" but not "expire" operation
        if ($msg != 'expired') {
            return;
        }
        $output[self::BUNDLE_KEY_PID] = getmypid();
        $output[self::BUNDLE_KEY_STATUS] = 1;
        $output[self::BUNDLE_KEY_PAYLOAD] = preg_replace('/^' . $this->subPattern . '/', '', $chan);
        echo json_encode($output) . '|';
    }

    public function monitorExpireKey($redisConf, $looper)
    {
        $rebirth = 3;
        while ($rebirth-- > 0) {
            try {
                echo 'Redis subscription task PID[' . getmypid() . "]\n";
                $redisConnectPub = RedisFactory::getRedisConn($redisConf);
                $this->keyEventMonitor($redisConnectPub);
            } catch (\RedisException $exception) {
                echo "Exception:$exception\n";
            } catch (CacheException $exception) {
                echo "Exception:$exception\n";
            }
        }
        $output[self::BUNDLE_KEY_PID] = getmypid();
        $output[self::BUNDLE_KEY_STATUS] = 0;
        echo json_encode($output) . '|';
    }

    public function performJobScheduler($redis, $looper)
    {
        echo 'InitJobScheduler PID[' . getmypid() . "]\n";
        $manager = new JobManager($looper, $this->pollingInterval);
        $looper->addTimer($this->executableTime, function () use (&$looper, &$executableTime) {
            echo "Force finishing scheduler after $executableTime second execution.\n";
            $looper->stop();
        });

        if (!isset($_SERVER['OS']) || strpos($_SERVER['OS'], 'Windows') === false) {
            $process = new \React\ChildProcess\Process($this->forkChildCmd);
            $process->start($looper);
            $process->stdout->on('data', function ($data) use (&$redis, &$manager, &$looper) {
                $this->handlePipeData($data, $redis, $manager, $looper);
            });
            $process->stderr->on('data', function ($data) use (&$looper) {
                echo 'Unexpected error in child process: ' . $data . '\nPrepare to quit...';
                $looper->stop();
            });
            $process->on('exit', function () use (&$looper) {
                $looper->stop();
                echo "Scheduler prepare to quit duo to death of child process.\n";
            });
        }

        $manager->run($redis, function ($key, $data, $way) {
            call_user_func($this->cb, $key, $data, $way);
        });
    }

    public function handlePipeData($data, $redis, $manager, $looper)
    {
        $array = explode('|', $data);
        echo "pipedata: $data\n";
        foreach ($array as $item) {
            $ipcBundle = json_decode($item, true);
            if (isset($ipcBundle[self::BUNDLE_KEY_PAYLOAD])) {
                $key = $ipcBundle[self::BUNDLE_KEY_PAYLOAD];
                $command = $manager->removeJobByScheduleKey($redis, $key);
                call_user_func($this->cb, $key, $command, 1);
            }
        }
    }
}
