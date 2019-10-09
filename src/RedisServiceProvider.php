<?php

namespace TapTap\LaravelRedis;

use Illuminate\Redis\RedisServiceProvider as LaravelRedisServiceProvider;
use Illuminate\Support\Arr;

class RedisServiceProvider extends LaravelRedisServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->app->singleton('redis', function ($app) {
            $config = $app->make('config')->get('database.redis');

            return new RedisManager(Arr::pull($config, 'client', 'predis'), $config);
        });

        $this->app->bind('redis.connection', function ($app) {
            return $app['redis']->connection();
        });
    }
}
