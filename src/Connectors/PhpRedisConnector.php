<?php

namespace TapTap\LaravelRedis\Connectors;

use Illuminate\Redis\Connectors\PhpRedisConnector as LaravelPhpRedisConnector;
use Illuminate\Support\Arr;
use Redis;
use RedisCluster;
use TapTap\LaravelRedis\Connections\PhpRedisClusterConnection;
use TapTap\LaravelRedis\Connections\PhpRedisConnection;

class PhpRedisConnector extends LaravelPhpRedisConnector
{
    /**
     * Create a new PhpRedis connection.
     *
     * @param array $config
     * @param array $options
     * @return PhpRedisConnection
     */
    public function connect(array $config, array $options)
    {
        return new PhpRedisConnection($this->createClient(array_merge(
            $config, $options, Arr::pull($config, 'options', [])
        )));
    }

    /**
     * Create a new clustered PhpRedis connection.
     *
     * @param array $config
     * @param array $clusterOptions
     * @param array $options
     * @return PhpRedisClusterConnection
     */
    public function connectToCluster(array $config, array $clusterOptions, array $options)
    {
        $options = array_merge($options, $clusterOptions, Arr::pull($config, 'options', []));

        return new PhpRedisClusterConnection($this->createRedisClusterInstance(
            array_map([$this, 'buildClusterConnectionString'], $config), $options
        ));
    }

    /**
     * Build a single cluster seed string from array.
     *
     * @param array $server
     * @return string
     */
    protected function buildClusterConnectionString(array $server)
    {
        return $server['host'].':'.$server['port'].'?'.http_build_query(Arr::only($server, [
                'database', 'password', 'prefix', 'read_timeout',
            ]));
    }

    /**
     * Create the Redis client instance.
     *
     * @param array $config
     * @return Redis
     */
    protected function createClient(array $config)
    {
        $client = new Redis();

        $this->establishConnection($client, $config);

        if (! empty($config['password'])) {
            $client->auth($config['password']);
        }
        if (! empty($config['database'])) {
            $client->select($config['database']);
        }

        if (! empty($config['prefix'])) {
            $client->setOption(Redis::OPT_PREFIX, $config['prefix']);
        }
        if (! empty($config['read_timeout'])) {
            $client->setOption(Redis::OPT_READ_TIMEOUT, $config['read_timeout']);
        }

        return $client;
    }

    /**
     * Establish a connection with the Redis host.
     *
     * @param \Redis $client
     * @param array $config
     * @return void
     */
    protected function establishConnection($client, array $config)
    {
        $persistent = $config['persistent'] ?? false;

        $parameters = [
            $config['host'],
            $config['port'],
            Arr::get($config, 'timeout', 0.0),
            $persistent ? Arr::get($config, 'persistent_id', null) : null,
            Arr::get($config, 'retry_interval', 0),
        ];

        if (version_compare(phpversion('redis'), '3.1.3', '>=')) {
            $parameters[] = Arr::get($config, 'read_timeout', 0.0);
        }

        $client->{($persistent ? 'pconnect' : 'connect')}(...$parameters);
    }

    /**
     * Create a new redis cluster instance.
     *
     * @param array $servers
     * @param array $options
     * @return RedisCluster
     */
    protected function createRedisClusterInstance(array $servers, array $options)
    {
        return new RedisCluster(
            null,
            array_values($servers),
            $options['timeout'] ?? 0,
            $options['read_timeout'] ?? 0,
            isset($options['persistent']) && $options['persistent']
        );
    }
}
