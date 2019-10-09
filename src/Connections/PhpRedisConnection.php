<?php

namespace TapTap\LaravelRedis\Connections;

use Predis\ClientInterface;
use Redis;
use TapTap\LaravelRedis\Laravel\PhpRedisConnection as LaravelPhpRedisConnection;

/**
 * Wrapper functions in order to match \Predis\ClientInterface
 *
 * @see ClientInterface
 * @see Redis
 */
class PhpRedisConnection extends LaravelPhpRedisConnection
{
    /**
     * @var Redis
     */
    protected $client;

    //
    // ~ Keys commands
    //

    /**
     * NOT completely match
     * 返回 $cursor string ("568") <=> int (568)
     *
     * @param int $cursor
     * @param array|null $options
     * @return array|bool
     */
    public function scan($cursor, array $options = null)
    {
        $this->client->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);

        if ($cursor === 0) {
            $cursor = null;
        }
        $result = $this->client->scan($cursor, '', $options['count'] ?? 0);

        return [$cursor, $result === false ? [] : $result];
    }

    /**
     * Determine if the given keys exist.
     *
     * @param mixed $keys
     * @return int
     */
    public function exists(...$keys)
    {
        return $this->client->exists(...$keys);
    }

    //
    // ~ Strings commands
    //

    /**
     * Returns the value of the given key.
     *
     * @param string $key
     * @return string|null
     */
    public function get($key)
    {
        $result = $this->command('get', [$key]);

        return $result !== false ? $result : null;
    }

    /**
     * Get the values of all the given keys.
     *
     * @param array $keys
     * @return array
     */
    public function mget(array $keys)
    {
        return array_map(function ($value) {
            return $value !== false ? $value : null;
        }, $this->command('mget', [$keys]));
    }

    /**
     * Set the string value in argument as value of the key.
     *
     * @param string $key
     * @param mixed $value
     * @param string|null $expireResolution
     * @param int|null $expireTTL
     * @param string|null $flag
     * @return bool
     */
    public function set($key, $value, $expireResolution = null, $expireTTL = null, $flag = null)
    {
        return $this->command('set', [
            $key,
            $value,
            $expireResolution ? [$flag, $expireResolution => $expireTTL] : null,
        ]);
    }

    /**
     * Set the given key if it doesn't exist.
     *
     * @param string $key
     * @param string $value
     * @return int
     */
    public function setnx($key, $value)
    {
        return (int) $this->command('setnx', [$key, $value]);
    }

    //
    // ~ Hashes commands
    //

    public function hmget($key, ...$fields)
    {
        if (count($fields) === 1 && is_array($fields[0])) {
            $result = $this->client->hMGet($key, $fields[0]);
        } else {
            $result = $this->client->hMGet($key, $fields);
        }

        return array_map(function ($item) {
            return $item === false ? null : $item;
        }, array_values($result));
    }

    public function hget($key, $field)
    {
        $result = $this->client->hGet($key, $field);

        return $result === false ? null : $result;
    }

    public function hdel($key, ...$fields)
    {
        if (count($fields) === 1 && is_array($fields[0])) {
            return $this->client->hDel($key, ...$fields[0]);
        }

        return $this->client->hDel($key, ...$fields);
    }

    /**
     * Set the given hash fields to their respective values.
     *
     * @param string $key
     * @param mixed $dictionary
     * @return int
     */
    public function hmset($key, ...$dictionary)
    {
        if (count($dictionary) === 1) {
            $dictionary = $dictionary[0];
        } else {
            $input = collect($dictionary);

            $dictionary = $input->nth(2)->combine($input->nth(2, 1))->toArray();
        }

        return $this->command('hmset', [$key, $dictionary]);
    }

    /**
     * Set the given hash field if it doesn't exist.
     *
     * @param string $hash
     * @param string $key
     * @param string $value
     * @return int
     */
    public function hsetnx($hash, $key, $value)
    {
        return (int) $this->command('hsetnx', [$hash, $key, $value]);
    }

    //
    // ~ Lists commands
    //

    /**
     * Removes the first count occurrences of the value element from the list.
     *
     * @param string $key
     * @param int $count
     * @param mixed $value
     * @return int|false
     */
    public function lrem($key, $count, $value)
    {
        return $this->command('lrem', [$key, $value, $count]);
    }

    public function lpush($key, ...$values)
    {
        if (count($values) === 1 && is_array($values[0])) {
            return $this->client->lPush($key, ...$values[0]);
        }

        return $this->client->lPush($key, ...$values);
    }

    public function rpush($key, ...$values)
    {
        if (count($values) === 1 && is_array($values[0])) {
            return $this->client->rPush($key, ...$values[0]);
        }

        return $this->client->rPush($key, ...$values);
    }

    public function lpop($key)
    {
        $result = $this->client->lPop($key);

        return $result === false ? null : $result;
    }

    public function rpop($key)
    {
        $result = $this->client->rPop($key);

        return $result === false ? null : $result;
    }

    /**
     * Removes and returns the first element of the list stored at key.
     *
     * @param mixed $arguments
     * @return array|null
     */
    public function blpop(...$arguments)
    {
        $result = $this->command('blpop', $arguments);

        return empty($result) ? null : $result;
    }

    /**
     * Removes and returns the last element of the list stored at key.
     *
     * @param mixed $arguments
     * @return array|null
     */
    public function brpop(...$arguments)
    {
        $result = $this->command('brpop', $arguments);

        return empty($result) ? null : $result;
    }

    //
    // ~ Sets commands
    //

    /**
     * Removes and returns a random element from the set value at key.
     *
     * @param string $key
     * @param int|null $count
     * @return mixed|false
     */
    public function spop($key, $count = null)
    {
        if ($count === null) {
            $result = $this->client->sPop($key);

            return $result === false ? null : $result;
        }

        $result = [];
        while ($count--) {
            $r = $this->client->sPop($key);
            if ($r === false) {
                break;
            }
            $result[] = $r;
        }

        return $result;
    }

    public function sadd($key, ...$members)
    {
        if (count($members) === 1 && is_array($members[0])) {
            return $this->client->sAddArray($key, $members[0]);
        }

        return $this->client->sAdd($key, ...$members);
    }

    public function srem($key, ...$member)
    {
        if (count($member) === 1 && is_array($member[0])) {
            return $this->client->sRem($key, ...$member[0]);
        }

        return $this->client->sRem($key, ...$member);
    }

    public function sunionstore($destination, array $keys)
    {
        if (count($keys) === 1) {
            $keys[] = '';
        }

        return $this->client->sUnionStore($destination, ...$keys);
    }

    public function sdiffstore($destination, array $keys)
    {
        if (count($keys) === 1) {
            $keys[] = '';
        }

        return $this->client->sDiffStore($destination, ...$keys);
    }

    //
    // ~ SortedSets commands
    //

    /**
     * Add one or more members to a sorted set or update its score if it already exists.
     *
     * @param string $key
     * @param mixed $dictionary
     * @return int
     */
    public function zadd($key, ...$dictionary)
    {
        if (count($dictionary) === 1) {
            $_dictionary = [];

            foreach ($dictionary[0] as $member => $score) {
                $_dictionary[] = $score;
                $_dictionary[] = $member;
            }

            $dictionary = $_dictionary;
        }

        return $this->client->zadd($key, ...$dictionary);
    }

    /**
     * NOT completely match
     * 返回 string ("4.4000000000000004") <=> float (4.4)
     *
     * @param string $key
     * @param string $member
     * @return float|null
     */
    public function zscore($key, $member)
    {
        $result = $this->client->zScore($key, $member);

        return $result === false ? null : $result;
    }

    public function zrevrank($key, $member)
    {
        $result = $this->client->zRevRank($key, $member);

        return $result === false ? null : $result;
    }

    public function zrem($key, ...$member)
    {
        if (count($member) === 1 && is_array($member[0])) {
            return $this->client->zRem($key, ...$member[0]);
        }

        return $this->client->zRem($key, ...$member);
    }

    /**
     * Return elements with score between $min and $max.
     *
     * @param string $key
     * @param mixed $min
     * @param mixed $max
     * @param array $options
     * @return array
     */
    public function zrangebyscore($key, $min, $max, $options = [])
    {
        return $this->client->zRangeByScore($key, $min, $max, $options);
    }

    /**
     * Return elements with score between $min and $max.
     *
     * @param string $key
     * @param mixed $min
     * @param mixed $max
     * @param array $options
     * @return array
     */
    public function zrevrangebyscore($key, $min, $max, $options = [])
    {
        return $this->client->zRevRangeByScore($key, $min, $max, $options);
    }

    /**
     * Find the intersection between sets and store in a new set.
     *
     * @param string $output
     * @param array $keys
     * @param array $options
     * @return int
     */
    public function zinterstore($output, $keys, $options = [])
    {
        return $this->command('zinterstore', [$output, $keys,
            $options['weights'] ?? null,
            $options['aggregate'] ?? 'sum',
        ]);
    }

    /**
     * Find the union between sets and store in a new set.
     *
     * @param string $output
     * @param array $keys
     * @param array $options
     * @return int
     */
    public function zunionstore($output, $keys, $options = [])
    {
        return $this->command('zunionstore', [$output, $keys,
            $options['weights'] ?? null,
            $options['aggregate'] ?? 'sum',
        ]);
    }

    public function zrange($key, $start, $stop, $options = null)
    {
        if ($options === 'withscores' || $options === 'WITHSCORES') {
            return $this->client->zRange($key, $start, $stop, true);
        }

        if ((isset($options['withscores']) && $options['withscores'])
            || (isset($options['WITHSCORES']) && $options['WITHSCORES'])) {
            return $this->client->zRange($key, $start, $stop, true);
        }

        return $this->client->zRange($key, $start, $stop, $options);
    }

    public function zrevrange($key, $start, $stop, $options = null)
    {
        if ($options === 'withscores' || $options === 'WITHSCORES') {
            return $this->client->zRevRange($key, $start, $stop, true);
        }

        if ((isset($options['withscores']) && $options['withscores'])
            || (isset($options['WITHSCORES']) && $options['WITHSCORES'])) {
            return $this->client->zRevRange($key, $start, $stop, true);
        }

        return $this->client->zRevRange($key, $start, $stop, $options);
    }

    /**
     * NOT completely match
     * 返回 $cursor string ("568") <=> int (568)
     *
     * @param string $key
     * @param int $cursor
     * @param array|null $options
     * @return array|bool
     */
    public function zscan($key, $cursor, array $options = null)
    {
        $this->client->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);

        if ($cursor === 0) {
            $cursor = null;
        }
        $result = $this->client->zScan($key, $cursor, '', $options['count'] ?? 0);

        return [$cursor, $result === false ? [] : $result];
    }
}
