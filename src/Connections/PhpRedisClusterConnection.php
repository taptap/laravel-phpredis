<?php

namespace TapTap\LaravelRedis\Connections;

use Redis;
use RedisCluster;

/**
 * Wrapper functions in order to match \Predis\ClientInterface
 *
 * @see ClientInterface
 * @see Redis
 * @see RedisCluster
 */
class PhpRedisClusterConnection extends PhpRedisConnection
{
    /**
     * @var RedisCluster
     */
    protected $client;

    public function exists(...$key)
    {
        $result = 0;

        if (count($key) === 1) {
            if (is_array($key[0])) {
                foreach ($key[0] as $k) {
                    $r = $this->client->exists($k);
                    if ($r) {
                        ++$result;
                    }
                }
            } else {
                $r = $this->client->exists($key[0]);
                if ($r) {
                    ++$result;
                }
            }
        } else {
            foreach ($key as $k) {
                $r = $this->client->exists($k);
                if ($r) {
                    ++$result;
                }
            }
        }

        return $result;
    }
}
