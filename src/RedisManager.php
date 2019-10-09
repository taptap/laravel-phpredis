<?php

namespace TapTap\LaravelRedis;

use Illuminate\Redis\Connectors\PredisConnector;
use Illuminate\Redis\RedisManager as LaravelRedisManager;
use TapTap\LaravelRedis\Connectors\PhpRedisConnector;

class RedisManager extends LaravelRedisManager
{
    protected function connector()
    {
        switch ($this->driver) {
            case 'predis':
                return new PredisConnector();
            case 'phpredis':
            default:
                return new PhpRedisConnector();
        }
    }
}
