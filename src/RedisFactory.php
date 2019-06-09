<?php

namespace TimeWorker;

class CacheException extends \Exception
{
    const REDIS_CONFIG_ERROR = "redis connection error...";
    const REDIS_CONTENT_FAIL = "redis content fail";
}

class RedisFactory
{
    public static function getRedisConn($redisConf)
    {
        return self::_initRedisPconnect($redisConf);
    }

    private static function _initRedisPconnect($redisConf)
    {
        // config socket time out 24 hours
        ini_set('default_socket_timeout', 86400);

        if (empty($redisConf['host']) || empty($redisConf['port'])) {
            throw new CacheException(CacheException::REDIS_CONFIG_ERROR);
        }

        $host = $redisConf['host'];
        $port = $redisConf['port'];
        $init = false;

        //check redis connect use ping not PONG reconnect
        if (!isset($redis)) {
            $init = true;
        } else {
            try {
                $redis->setOption(\Redis::OPT_READ_TIMEOUT, 1);
                $redis->ping();
            } catch (\Exception $exception) {
                $redis->close();
                unset($redis);
                $init = true;
            }
        }

        // init redis
        if ($init) {
            $index = 1;
            $retry = 3;

            while ($retry-- > 0) {
                try {
                    $redis = new \Redis();
                    $result = $redis->pconnect($host, $port, 2); //设置pconnect超时时间为1秒
                    if ($result == false) {
                        throw new \Exception('pconnect fail');
                    }
                    if (isset($redisConf['passwd'])) {
                        $ret = $redis->auth($redisConf['passwd']);
                        if (!$ret) {
                            throw new CacheException(CacheException::REDIS_CONTENT_FAIL);
                        }
                    }

                    if (isset($redisConf['db'])) {
                        $redis->select($redisConf['db']);
                    }
                    $redis->setOption(\Redis::OPT_READ_TIMEOUT, 1);
                    $redis->ping();
                    break;
                } catch (\Exception $exception) {
                    print_r("redis info", "redis $host:$port pconnect or ping fail $index times:" . $exception->getMessage());
                    $redis->close();
                    unset($redis);
                }

                $index++;
            }
            if ($retry <= 0) {
                print_r("redis fail", "redis $host:$port pconnect and ping fail after try 3 times");
                throw new CacheException(CacheException::REDIS_CONTENT_FAIL);
            }
        }
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, 3);
        if (isset($redisConf['db'])) {
            $redis->select($redisConf['db']);
        } else {
            $redis->select(0); // 默认 DB
        }

        return $redis;
    }
}
