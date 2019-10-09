<?php

namespace Tests\Unit\Libraries;

use Illuminate\Redis\Connectors\PredisConnector;
use PHPUnit\Framework\TestCase;
use TapTap\LaravelRedis\Connections\PhpRedisClusterConnection;
use TapTap\LaravelRedis\Connectors\PhpRedisConnector;

/**
 * Tests for Redis
 */
class RedisTest extends TestCase
{
    public function redis()
    {
        return [
            [
                (new PhpRedisConnector())->connect([
                    'host' => '127.0.0.1',
                    'port' => '6379',
                ], []),
            ],
            [
                (new PhpRedisConnector())->connectToCluster([
                    [
                        'host' => '127.0.0.1',
                        'port' => '7000',
                    ],
                ], [], []),
            ],
            [
                (new PredisConnector())->connect([
                    'host' => '127.0.0.1',
                    'port' => '6379',
                ], []),
            ],
        ];
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testTemplate($redis)
    {
        $key = '??';

        $redis->del($key);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testMultiExec($redis)
    {
        $key1 = '?multi?';
        $key2 = '?exec?';

        $redis->del($key1, $key2);

        $redis->multi();
        $redis->set($key1, 1);
        $redis->set($key2, 'x');
        $redis->exec();

        $this->assertEquals(1, $redis->get($key1));
        $this->assertEquals('x', $redis->get($key2));

        $redis->del($key1, $key2);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testKeys($redis)
    {
        $key = '?keys?';

        $i = 1000;
        while ($i--) {
            $redis->del($key.$i);
        }

        $r = $redis->keys('*?keys?*');
        $this->assertListEquals([], $r);

        $i = 1000;
        while ($i--) {
            $redis->set($key.$i, $i);
        }
        $r = $redis->keys('*?keys?*');
        $this->assertEquals(1000, count($r));

        $i = 1000;
        while ($i--) {
            $redis->del($key.$i);
        }
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testScan($redis)
    {
        $cnt1 = 0;
        $cursor = 0;
        do {
            list($cursor, $r) = $redis->scan($cursor);
            $cnt1 += count($r);
        } while ($cursor);

        $cnt2 = 0;
        $cursor = 0;
        do {
            list($cursor, $r) = $redis->scan($cursor, ['count' => 100]);
            $cnt2 += count($r);
        } while ($cursor);

        $this->assertEquals($cnt1, $cnt2);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testExists($redis)
    {
        $keys = ['?exists-1?', '?exists-2?'];

        $redis->del($keys);

        $r = $redis->exists(...$keys);
        $this->assertEquals(0, $r);

        $redis->set($keys[0], 1);
        $redis->set($keys[1], 2);

        $r = $redis->exists(...$keys);
        $this->assertEquals(2, $r);
        $r = $redis->exists($keys[0]);
        $this->assertEquals(1, $r);

        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testExpire($redis)
    {
        $key = '?expire?';

        $redis->del($key);

        $r = $redis->expire($key, 1);
        $this->assertFalse((bool) $r);

        $redis->set($key, 1);
        $r = $redis->expire($key, 1);
        $this->assertTrue((bool) $r);

        sleep(2);
        $this->assertNull($redis->get($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testGet($redis)
    {
        $key = '?get?';

        $redis->del($key);

        $r = $redis->get($key);
        $this->assertNull($r);

        $redis->set($key, 1);
        $r = $redis->get($key);
        $this->assertEquals(1, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSet($redis)
    {
        $key = '?set?';

        $redis->del($key);

        $r = $redis->set($key, 123);
        $this->assertTrue((bool) $r);
        $this->assertEquals(123, $redis->get($key));

        $redis->del($key);

        $r = $redis->set($key, 123, 'EX', 1, 'NX');
        $this->assertTrue((bool) $r);
        $this->assertEquals(123, $redis->get($key));
        $r = $redis->set($key, 123, 'EX', 1, 'NX');
        $this->assertFalse((bool) $r);
        sleep(2);
        $r = $redis->set($key, 123, 'EX', 1, 'NX');
        $this->assertTrue((bool) $r);
        $this->assertEquals(123, $redis->get($key));

        $redis->del($key);

        $r = $redis->set($key, 123, 'EX', 1, 'XX');
        $this->assertFalse((bool) $r);
        $r = $redis->set($key, 123);
        $r = $redis->set($key, 123, 'EX', 1, 'XX');
        $this->assertTrue((bool) $r);
        $this->assertEquals(123, $redis->get($key));
        sleep(2);
        $r = $redis->set($key, 123, 'EX', 1, 'XX');
        $this->assertFalse((bool) $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSetNx($redis)
    {
        $key = '?setnx?';

        $redis->del($key);

        $r = $redis->setnx($key, 123);
        $this->assertTrue((bool) $r);
        $this->assertEquals(123, $redis->get($key));

        $r = $redis->setnx($key, 321);
        $this->assertFalse((bool) $r);
        $this->assertEquals(123, $redis->get($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testDel($redis)
    {
        $redis->del('a', 'b');

        $r1 = $redis->del('a', 'b');
        $this->assertEquals(0, $r1);

        $redis->set('a', 1);
        $redis->set('b', 1);
        $r2 = $redis->del('a', 'b');
        $this->assertEquals(2, $r2);

        $redis->set('a', 1);
        $redis->set('b', 1);
        $r3 = $redis->del(['a', 'b']);
        $this->assertEquals(2, $r3);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testIncr($redis)
    {
        $key = '?incr?';

        $redis->del($key);

        $r = $redis->incr($key);
        $this->assertEquals(1, $r);

        $redis->set($key, 5);
        $r = $redis->incr($key);
        $this->assertEquals(6, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testDecr($redis)
    {
        $key = '?decr?';

        $redis->del($key);

        $r = $redis->decr($key);
        $this->assertEquals(-1, $r);

        $redis->set($key, 5);
        $r = $redis->decr($key);
        $this->assertEquals(4, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testIncrBy($redis)
    {
        $key = '?incrby?';

        $redis->del($key);

        $r = $redis->incrby($key, 1);
        $this->assertEquals(1, $r);

        $r = $redis->incrby($key, 10);
        $this->assertEquals(11, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testDecrBy($redis)
    {
        $key = '?decrby?';

        $redis->del($key);

        $r = $redis->decrby($key, 1);
        $this->assertEquals(-1, $r);

        $r = $redis->decrby($key, 10);
        $this->assertEquals(-11, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testMGet($redis)
    {
        $keys = ['?mget1?', '?mget2?', '?mget3?'];

        $redis->del($keys);

        $r = $redis->mget($keys);
        $this->assertListEquals([null, null, null], $r);

        $redis->set($keys[0], 1);
        $redis->set($keys[1], 2);
        $redis->set($keys[2], 3);
        $r = $redis->mget($keys);
        $this->assertListEquals([1, 2, 3], $r);

        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testMSet($redis)
    {
        $keys = ['?mset1?', '?mset2?', '?mset3?'];

        $redis->del($keys);

        $r = $redis->mset([
            $keys[0] => 1,
            $keys[1] => 2,
            $keys[2] => 3,
        ]);
        $this->assertTrue((bool) $r);
        $this->assertEquals(1, $redis->get($keys[0]));
        $this->assertEquals(2, $redis->get($keys[1]));
        $this->assertEquals(3, $redis->get($keys[2]));

        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testLLen($redis)
    {
        $key = '?llen?';

        $redis->del($key);

        $r = $redis->llen($key);
        $this->assertEquals(0, $r);

        $redis->lpush($key, ['v1', 'v2', 'v3']);
        $r = $redis->llen($key);
        $this->assertEquals(3, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testLPush($redis)
    {
        $key = '?lpush?';

        $redis->del($key);

        $r = $redis->lpush($key, ['v1', 'v2', 'v3']);
        $this->assertEquals(3, $r);
        $this->assertEquals(3, $redis->llen($key));
        $r = $redis->lpush($key, ['v1', 'v2', 'v3']);
        $this->assertEquals(6, $r);
        $this->assertEquals(6, $redis->llen($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testRPush($redis)
    {
        $key = '?rpush?';

        $redis->del($key);

        $r = $redis->rpush($key, ['v1', 'v2', 'v3']);
        $this->assertEquals(3, $r);
        $this->assertEquals(3, $redis->llen($key));
        $r = $redis->rpush($key, ['v1', 'v2', 'v3']);
        $this->assertEquals(6, $r);
        $this->assertEquals(6, $redis->llen($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testLPop($redis)
    {
        $key = '?lpop?';

        $redis->del($key);

        $r = $redis->lpop($key);
        $this->assertNull($r);

        $redis->lpush($key, ['v1', 'v2', 'v3']);

        $r = $redis->lpop($key);
        $this->assertEquals('v3', $r);
        $r = $redis->lpop($key);
        $this->assertEquals('v2', $r);
        $r = $redis->lpop($key);
        $this->assertEquals('v1', $r);
        $r = $redis->lpop($key);
        $this->assertNull($r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testRPop($redis)
    {
        $key = '?rpop?';

        $redis->del($key);

        $r = $redis->rpop($key);
        $this->assertNull($r);

        $redis->lpush($key, ['v1', 'v2', 'v3']);

        $r = $redis->rpop($key);
        $this->assertEquals('v1', $r);
        $r = $redis->rpop($key);
        $this->assertEquals('v2', $r);
        $r = $redis->rpop($key);
        $this->assertEquals('v3', $r);
        $r = $redis->rpop($key);
        $this->assertNull($r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testBLPop($redis)
    {
        $key = '?blpop?';
        $keys = ['?blpop-1?', '?blpop-2?', '?blpop-3?'];

        $redis->del($key);
        $redis->del($keys);

        // test blpop on non-exists key
        $r = $redis->blpop($key, 1);
        $this->assertNull($r);

        // test blpop key
        $redis->lpush($key, ['v1', 'v2', 'v3']);

        $r = $redis->blpop($key, 1);
        $this->assertListEquals([$key, 'v3'], $r);
        $r = $redis->blpop($key, 1);
        $this->assertListEquals([$key, 'v2'], $r);
        $r = $redis->blpop($key, 1);
        $this->assertListEquals([$key, 'v1'], $r);
        $r = $redis->blpop($key, 1);
        $this->assertNull($r);

        // test blpop keys
        if (! $redis instanceof PhpRedisClusterConnection) {
            $r = $redis->blpop($keys, 1);
            $this->assertNull($r);
            $r = $redis->blpop($keys[0], $keys[1], $keys[2], 1);
            $this->assertNull($r);

            $redis->lpush($keys[0], ['v1', 'v2', 'v3']);
            $redis->lpush($keys[1], ['v1', 'v2', 'v3']);
            $redis->lpush($keys[2], ['v1', 'v2', 'v3']);

            $r = $redis->blpop($keys, 1);
            $this->assertListEquals([$keys[0], 'v3'], $r);
            $r = $redis->blpop($keys[1], $keys[2], 1);
            $this->assertListEquals([$keys[1], 'v3'], $r);
            $r = $redis->blpop($keys[2], $keys[0], 1);
            $this->assertListEquals([$keys[2], 'v3'], $r);
            $r = $redis->blpop($keys, 1);
            $this->assertListEquals([$keys[0], 'v2'], $r);
        }

        $redis->del($key);
        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testBRPop($redis)
    {
        $key = '?brpop?';
        $keys = ['?brpop-1?', '?brpop-2?', '?brpop-3?'];

        $redis->del($key);
        $redis->del($keys);

        // test blpop on non-exists key
        $r = $redis->brpop($key, 1);
        $this->assertNull($r);

        // test blpop key
        $redis->lpush($key, ['v1', 'v2', 'v3']);

        $r = $redis->brpop($key, 1);
        $this->assertListEquals([$key, 'v1'], $r);
        $r = $redis->brpop($key, 1);
        $this->assertListEquals([$key, 'v2'], $r);
        $r = $redis->brpop($key, 1);
        $this->assertListEquals([$key, 'v3'], $r);
        $r = $redis->brpop($key, 1);
        $this->assertNull($r);

        // test blpop keys
        if (! $redis instanceof PhpRedisClusterConnection) {
            $r = $redis->brpop($keys, 1);
            $this->assertNull($r);
            $r = $redis->brpop($keys[0], $keys[1], $keys[2], 1);
            $this->assertNull($r);

            $redis->lpush($keys[0], ['v1', 'v2', 'v3']);
            $redis->lpush($keys[1], ['v1', 'v2', 'v3']);
            $redis->lpush($keys[2], ['v1', 'v2', 'v3']);

            $r = $redis->brpop($keys, 1);
            $this->assertListEquals([$keys[0], 'v1'], $r);
            $r = $redis->brpop($keys[1], $keys[2], 1);
            $this->assertListEquals([$keys[1], 'v1'], $r);
            $r = $redis->brpop($keys[2], $keys[0], 1);
            $this->assertListEquals([$keys[2], 'v1'], $r);
            $r = $redis->brpop($keys, 1);
            $this->assertListEquals([$keys[0], 'v2'], $r);
        }

        $redis->del($key);
        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testLRem($redis)
    {
        $key = '?lrem?';

        $redis->del($key);

        // test lrem by count
        $redis->rpush($key, ['v1', 'v2', 'v3', 'v1', 'v1']);

        $r = $redis->lrem($key, 2, 'v1');
        $this->assertEquals(2, $r);
        $r = $redis->llen($key);
        $this->assertEquals(3, $r);
        $r = $redis->lrange($key, 0, -1);
        $this->assertListEquals(['v2', 'v3', 'v1'], $r);

        $redis->del($key);

        // test lrem by a huge count
        $redis->rpush($key, ['v1', 'v2', 'v3', 'v1', 'v1']);

        $r = $redis->lrem($key, 5, 'v1');
        $this->assertEquals(3, $r);
        $r = $redis->llen($key);
        $this->assertEquals(2, $r);
        $r = $redis->lrange($key, 0, -1);
        $this->assertListEquals(['v2', 'v3'], $r);

        $redis->del($key);

        // test lrem by 0
        $redis->rpush($key, ['v1', 'v2', 'v3', 'v1', 'v1']);

        $r = $redis->lrem($key, 0, 'v1');
        $this->assertEquals(3, $r);
        $r = $redis->llen($key);
        $this->assertEquals(2, $r);
        $r = $redis->lrange($key, 0, -1);
        $this->assertListEquals(['v2', 'v3'], $r);

        $redis->del($key);

        // test lrem by negative number
        $redis->rpush($key, ['v1', 'v2', 'v3', 'v1', 'v1']);

        $r = $redis->lrem($key, -1, 'v1');
        $this->assertEquals(1, $r);
        $r = $redis->llen($key);
        $this->assertEquals(4, $r);
        $r = $redis->lrange($key, 0, -1);
        $this->assertListEquals(['v1', 'v2', 'v3', 'v1'], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testLTrim($redis)
    {
        $key = '?ltrim?';

        $redis->del($key);

        $r = $redis->ltrim($key, 0, -1);
        $this->assertTrue((bool) $r);

        $redis->lpush($key, ['v1', 'v2', 'v3']);

        $r = $redis->ltrim($key, 0, -1);
        $this->assertTrue((bool) $r);
        $this->assertListEquals(['v3', 'v2', 'v1'], $redis->lrange($key, 0, -1));
        $r = $redis->ltrim($key, -2, -1);
        $this->assertTrue((bool) $r);
        $this->assertListEquals(['v2', 'v1'], $redis->lrange($key, 0, -1));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSAdd($redis)
    {
        $key = '?sadd?';

        $redis->del($key);

        $num = $redis->sadd($key, [1, 2, 3]);
        $this->assertEquals(3, $num);

        $num = $redis->sadd($key, [4]);
        $this->assertEquals(1, $num);

        $num = $redis->sadd($key, [1, 5, 6]);
        $this->assertEquals(2, $num);

        $num = $redis->sadd($key, 7);
        $this->assertEquals(1, $num);

        $num = $redis->sadd($key, 8, 9, 10);
        $this->assertEquals(3, $num);

        $result = $redis->smembers($key);
        $this->assertListEquals($result, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSCard($redis)
    {
        $key = '?scard?';

        $redis->del($key);

        $r = $redis->scard($key);
        $this->assertEquals(0, $r);

        $r = $redis->sadd($key, ['v1', 'v2', 'v3', 'v3']);
        $this->assertEquals(3, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSIsMember($redis)
    {
        $key = '?sismember?';

        $redis->del($key);

        $r = $redis->sismember($key, 'v2');
        $this->assertFalse((bool) $r);

        $redis->sadd($key, ['v1', 'v2', 'v3', 'v4']);

        $r = $redis->sismember($key, 'v2');
        $this->assertTrue((bool) $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSMembers($redis)
    {
        $key = '?smembers?';

        $redis->del($key);

        $r = $redis->smembers($key);
        $this->assertListEquals([], $r);

        $redis->sadd($key, ['v1', 'v2', 'v3', 'v4']);

        $r = $redis->smembers($key);
        $this->assertSetEquals(['v1', 'v2', 'v3', 'v4'], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSPop($redis)
    {
        $key = '?spop?';

        $redis->del($key);

        $r = $redis->spop($key);
        $this->assertNull($r);
        $r = $redis->spop($key, 2);
        $this->assertListEquals([], $r);

        $redis->sadd($key, ['v1', 'v2', 'v3', 'v4']);

        $r = $redis->spop($key);
        $this->assertNotNull($r);
        $r = $redis->spop($key, 2);
        $this->assertEquals(2, count($r));
        $r = $redis->spop($key, 2);
        $this->assertEquals(1, count($r));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSRem($redis)
    {
        $key = '?srem?';

        $redis->del($key);

        $r = $redis->srem($key, 'v1');
        $this->assertEquals(0, $r);

        $redis->sadd($key, ['v1', 'v2', 'v3', 'v4', 'v5', 'v6']);

        $r = $redis->srem($key, 'v1');
        $this->assertEquals(1, $r);
        $this->assertEquals(5, $redis->scard($key));
        $r = $redis->srem($key, 'v2', 'v3');
        $this->assertEquals(2, $r);
        $this->assertEquals(3, $redis->scard($key));
        $r = $redis->srem($key, ['v2', 'v3']);
        $this->assertEquals(0, $r);
        $this->assertEquals(3, $redis->scard($key));
        $r = $redis->srem($key, ['v4', 'v5']);
        $this->assertEquals(2, $r);
        $this->assertEquals(1, $redis->scard($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSUnionStore($redis)
    {
        if ($redis instanceof PhpRedisClusterConnection) {
            return; // skip test if redis in cluster mode
        }

        $key = '?sunionstore?';
        $keys = ['?sunionstore-1?', '?sunionstore-2?', '?sunionstore-3?'];

        $redis->del($key);
        $redis->del($keys);

        $r = $redis->sunionstore($key, [$key]);
        $this->assertEquals(0, $r);
        $this->assertSetEquals([], $redis->smembers($key));

        $redis->sadd($keys[0], ['v1', 'v2']);
        $redis->sadd($keys[1], ['v1', 'v3']);
        $redis->sadd($keys[2], ['v4', 'v5']);

        $r = $redis->sunionstore($key, $keys);
        $this->assertEquals(5, $r);
        $this->assertSetEquals(['v1', 'v2', 'v3', 'v4', 'v5'], $redis->smembers($key));

        $r = $redis->sunionstore($key, [$key]);
        $this->assertEquals(5, $r);
        $this->assertSetEquals(['v1', 'v2', 'v3', 'v4', 'v5'], $redis->smembers($key));

        $redis->del($key);
        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testSDiffStore($redis)
    {
        if ($redis instanceof PhpRedisClusterConnection) {
            return; // skip test if redis in cluster mode
        }

        $key = '?sdiffstore?';
        $keys = ['?sdiffstore-1?', '?sdiffstore-2?', '?sdiffstore-3?'];

        $redis->del($key);
        $redis->del($keys);

        $r = $redis->sdiffstore($key, [$key]);
        $this->assertEquals(0, $r);
        $this->assertSetEquals([], $redis->smembers($key));

        $redis->sadd($keys[0], ['v1', 'v2', 'v3']);
        $redis->sadd($keys[1], ['v1', 'v4']);
        $redis->sadd($keys[2], ['v5']);

        $r = $redis->sdiffstore($key, $keys);
        $this->assertEquals(2, $r);
        $this->assertSetEquals(['v2', 'v3'], $redis->smembers($key));

        $r = $redis->sdiffstore($key, [$key]);
        $this->assertEquals(2, $r);
        $this->assertSetEquals(['v2', 'v3'], $redis->smembers($key));

        $redis->del($key);
        $redis->del($keys);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZAdd($redis)
    {
        $key = '?zadd?';

        $redis->del($key);

        $r = $redis->zadd($key, 0, 'v0', 1, 'v1', 2, 'v2', 3, 'v3');
        $this->assertEquals(4, $r);
        $this->assertListEquals($redis->zrange($key, 0, -1), ['v0', 'v1', 'v2', 'v3']);

        $r = $redis->zadd($key, 0, 'v0');
        $this->assertEquals(0, $r);

        $r = $redis->zadd($key, 4, 'v4');
        $this->assertEquals(1, $r);
        $this->assertListEquals($redis->zrange($key, 0, -1), ['v0', 'v1', 'v2', 'v3', 'v4']);

        $redis->del($key);

        $r = $redis->zadd($key, [
            'v0' => 0,
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $this->assertEquals(4, $r);
        $this->assertListEquals($redis->zrange($key, 0, -1), ['v0', 'v1', 'v2', 'v3']);

        $r = $redis->zadd($key, ['v0' => 0]);
        $this->assertEquals(0, $r);

        $r = $redis->zadd($key, ['v4' => 4]);
        $this->assertEquals(1, $r);
        $this->assertListEquals($redis->zrange($key, 0, -1), ['v0', 'v1', 'v2', 'v3', 'v4']);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRem($redis)
    {
        $key = '?zrem?';

        $redis->del($key);

        $r = $redis->zrem($key, 'v1');
        $this->assertEquals(0, $r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
            'v4' => 4,
            'v5' => 5,
            'v6' => 6,
        ]);

        $r = $redis->zrem($key, 'v1');
        $this->assertEquals(1, $r);
        $this->assertEquals(5, $redis->zcard($key));
        $r = $redis->zrem($key, 'v2', 'v3');
        $this->assertEquals(2, $r);
        $this->assertEquals(3, $redis->zcard($key));
        $r = $redis->zrem($key, ['v2', 'v3']);
        $this->assertEquals(0, $r);
        $this->assertEquals(3, $redis->zcard($key));
        $r = $redis->zrem($key, ['v4', 'v5']);
        $this->assertEquals(2, $r);
        $this->assertEquals(1, $redis->zcard($key));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZCard($redis)
    {
        $key = '?zcard?';

        $redis->del($key);

        $r = $redis->zcard($key);
        $this->assertEquals(0, $r);

        $redis->zadd($key, [
            'v0' => 0,
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $r = $redis->zcard($key);
        $this->assertEquals(4, $r);
        $redis->zrem($key, 'v2');
        $r = $redis->zcard($key);
        $this->assertEquals(3, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZScore($redis)
    {
        $key = '?zscore?';

        $redis->del($key);

        $r = $redis->zscore($key, 'v1');
        $this->assertNull($r);

        $redis->zadd($key, ['v1' => 1]);
        $r = $redis->zscore($key, 'v1');
        $this->assertEquals(1, $r);

        $redis->zincrby($key, 1, 'v1');
        $r = $redis->zscore($key, 'v1');
        $this->assertEquals(2, $r);

        $redis->zincrby($key, 1.3, 'v1');
        $r = $redis->zscore($key, 'v1');
        $this->assertEquals(3.3, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZIncrBy($redis)
    {
        $key = '?zincrby?';

        $redis->del($key);

        $r = $redis->zincrby($key, 1, 'v1');
        $this->assertEquals(1, (float) $r);
        $this->assertEquals(1, (int) $r);
        $this->assertEquals(1, $redis->zscore($key, 'v1'));

        $r = $redis->zincrby($key, 1, 'v1');
        $this->assertEquals(2, (float) $r);
        $this->assertEquals(2, (int) $r);
        $this->assertEquals(2, $redis->zscore($key, 'v1'));

        $r = $redis->zincrby($key, 2.1, 'v1');
        $this->assertEquals(4.1, (float) $r);
        $this->assertEquals(4, (int) $r);
        $this->assertEquals(4.1, $redis->zscore($key, 'v1'));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRevRank($redis)
    {
        $key = '?zrevrank?';

        $redis->del($key);

        $r = $redis->zrevrank($key, 'v3');
        $this->assertNull($r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);

        $r = $redis->zrevrank($key, 'v3');
        $this->assertEquals(0, $r);
        $r = $redis->zrevrank($key, 'v2');
        $this->assertEquals(1, $r);
        $r = $redis->zrevrank($key, 'v1');
        $this->assertEquals(2, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZCount($redis)
    {
        $key = '?zcount?';

        $redis->del($key);

        $r = $redis->zcount($key, 0, 100);
        $this->assertEquals(0, $r);

        $redis->zadd($key, [
            'v0' => 0,
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);

        $r = $redis->zcount($key, 0, 100);
        $this->assertEquals(4, $r);
        $r = $redis->zcount($key, 0, 1);
        $this->assertEquals(2, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRange($redis)
    {
        $key = '?zrange?';

        $redis->del($key);

        $r = $redis->zrange($key, 0, -1);
        $this->assertListEquals([], $r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $r = $redis->zrange($key, 0, -1);
        $this->assertListEquals(['v1', 'v2', 'v3'], $r);

        $r = $redis->zrange($key, -2, -1);
        $this->assertListEquals(['v2', 'v3'], $r);

        $r = $redis->zrange($key, 0, -1, [
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, 0, -1, [
            'WITHSCORES' => true,
        ]);
        $this->assertSortedMapEquals([
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, 0, -1, 'withscores');
        $this->assertSortedMapEquals([
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, 0, -1, 'WITHSCORES');
        $this->assertSortedMapEquals([
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ], $r);

        $r = $redis->zrange($key, -2, -1, [
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, -2, -1, [
            'WITHSCORES' => true,
        ]);
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, -2, -1, 'withscores');
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v3' => 3,
        ], $r);
        $r = $redis->zrange($key, -2, -1, 'WITHSCORES');
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v3' => 3,
        ], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRevRange($redis)
    {
        $key = '?zrevrange?';

        $redis->del($key);

        $r = $redis->zrevrange($key, 0, -1);
        $this->assertListEquals([], $r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $r = $redis->zrevrange($key, 0, -1);
        $this->assertListEquals(['v3', 'v2', 'v1'], $r);

        $r = $redis->zrevrange($key, -2, -1);
        $this->assertListEquals(['v2', 'v1'], $r);

        $r = $redis->zrevrange($key, 0, -1, [
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v3' => 3,
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, 0, -1, [
            'WITHSCORES' => true,
        ]);
        $this->assertSortedMapEquals([
            'v3' => 3,
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, 0, -1, 'withscores');
        $this->assertSortedMapEquals([
            'v3' => 3,
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, 0, -1, 'WITHSCORES');
        $this->assertSortedMapEquals([
            'v3' => 3,
            'v2' => 2,
            'v1' => 1,
        ], $r);

        $r = $redis->zrevrange($key, -2, -1, [
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, -2, -1, [
            'WITHSCORES' => true,
        ]);
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, -2, -1, 'withscores');
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v1' => 1,
        ], $r);
        $r = $redis->zrevrange($key, -2, -1, 'WITHSCORES');
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v1' => 1,
        ], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRangeByScore($redis)
    {
        $key = '?zrangebyscore?';

        $redis->del($key);

        $r = $redis->zrangebyscore($key, 0, -1);
        $this->assertListEquals([], $r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
            'v4' => 4,
        ]);
        $r = $redis->zrangebyscore($key, 1, 3);
        $this->assertListEquals(['v1', 'v2', 'v3'], $r);

        $r = $redis->zrangebyscore($key, 2, '+inf');
        $this->assertListEquals(['v2', 'v3', 'v4'], $r);

        $r = $redis->zrangebyscore($key, '-inf', '+inf');
        $this->assertListEquals(['v1', 'v2', 'v3', 'v4'], $r);

        $r = $redis->zrangebyscore($key, '-inf', '+inf', [
            'limit' => [1, 2],
        ]);
        $this->assertListEquals(['v2', 'v3'], $r);

        $r = $redis->zrangebyscore($key, '-inf', '+inf', [
            'limit'      => [1, 2],
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v2' => 2,
            'v3' => 3,
        ], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZRevRangeByScore($redis)
    {
        $key = '?zrevrangebyscore?';

        $redis->del($key);

        $r = $redis->zrevrangebyscore($key, 0, -1);
        $this->assertListEquals([], $r);

        $redis->zadd($key, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
            'v4' => 4,
        ]);
        $r = $redis->zrevrangebyscore($key, 3, 1);
        $this->assertListEquals(['v3', 'v2', 'v1'], $r);

        $r = $redis->zrevrangebyscore($key, '+inf', 2);
        $this->assertListEquals(['v4', 'v3', 'v2'], $r);

        $r = $redis->zrevrangebyscore($key, '+inf', '-inf');
        $this->assertListEquals(['v4', 'v3', 'v2', 'v1'], $r);

        $r = $redis->zrevrangebyscore($key, '+inf', '-inf', [
            'limit' => [1, 2],
        ]);
        $this->assertListEquals(['v3', 'v2'], $r);

        $r = $redis->zrevrangebyscore($key, '+inf', '-inf', [
            'limit'      => [1, 2],
            'withscores' => true,
        ]);
        $this->assertSortedMapEquals([
            'v3' => 3,
            'v2' => 2,
        ], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZScan($redis)
    {
        $key = '?zscan?';

        $redis->del($key);

        list($cursor, $r) = $redis->zscan($key, 0);
        $this->assertEquals(0, $cursor);
        $this->assertListEquals([], $r);

        $i = 1000;
        while ($i--) {
            $redis->zadd($key, [
                'v'.$i => $i,
            ]);
        }

        $cnt = 0;
        $cursor = 0;
        do {
            list($cursor, $r) = $redis->zscan($key, $cursor);
            foreach ($r as $i) {
                ++$cnt;
            }
        } while ($cursor);
        $this->assertEquals(1000, $cnt);

        $cnt = 0;
        $cursor = 0;
        do {
            list($cursor, $r) = $redis->zscan($key, $cursor, ['count' => 100]);
            foreach ($r as $i) {
                ++$cnt;
            }
        } while ($cursor);
        $this->assertEquals(1000, $cnt);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZInterStore($redis)
    {
        if ($redis instanceof PhpRedisClusterConnection) {
            return;
        }

        $key = '?zinterstore?';
        $key1 = '?zinterstore-1?';
        $key2 = '?zinterstore-2?';

        $redis->del($key, $key1, $key2);

        $redis->zadd($key1, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $redis->zadd($key2, [
            'v1' => 4,
            'v3' => 6,
        ]);

        $r = $redis->zinterstore($key, [$key1, $key2]);
        $this->assertEquals(2, $r);
        $this->assertHashMapEquals(['v1' => 5, 'v3' => 9], $redis->zrange($key, 0, -1, 'WITHSCORES'));

        $redis->del($key);
        $r = $redis->zinterstore($key, [$key1, $key2], [
            'weights'   => [3, 1],
            'aggregate' => 'max',
        ]);
        $this->assertEquals(2, $r);
        $this->assertHashMapEquals(['v1' => 4, 'v3' => 9], $redis->zrange($key, 0, -1, 'WITHSCORES'));

        $redis->del($key, $key1, $key2);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testZUnionStore($redis)
    {
        if ($redis instanceof PhpRedisClusterConnection) {
            return;
        }

        $key = '?zunionstore?';
        $key1 = '?zunionstore-1?';
        $key2 = '?zunionstore-2?';

        $redis->del($key, $key1, $key2);

        $redis->zadd($key1, [
            'v1' => 1,
            'v2' => 2,
            'v3' => 3,
        ]);
        $redis->zadd($key2, [
            'v1' => 4,
            'v3' => 6,
        ]);

        $r = $redis->zunionstore($key, [$key1, $key2]);
        $this->assertEquals(3, $r);
        $this->assertHashMapEquals(['v1' => 5, 'v2' => 2, 'v3' => 9], $redis->zrange($key, 0, -1, 'WITHSCORES'));

        $redis->del($key);
        $r = $redis->zunionstore($key, [$key1, $key2], [
            'weights'   => [3, 1],
            'aggregate' => 'max',
        ]);
        $this->assertEquals(3, $r);
        $this->assertHashMapEquals(['v1' => 4, 'v2' => 6, 'v3' => 9], $redis->zrange($key, 0, -1, 'WITHSCORES'));

        $redis->del($key, $key1, $key2);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHGetAll($redis)
    {
        $key = '?hgetall?';

        $redis->del($key);

        $r = $redis->hgetall($key);
        $this->assertListEquals([], $r);

        $redis->hset($key, 'k1', 'v1');
        $redis->hset($key, 'k2', 'v2');
        $redis->hset($key, 'k3', 'v3');
        $r = $redis->hgetall($key);
        $this->assertHashMapEquals([
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
        ], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHGet($redis)
    {
        $key = '?hget?';

        $redis->del($key);

        $r = $redis->hget($key, 'k1');
        $this->assertNull($r);

        $redis->hset($key, 'k1', 'v1');
        $redis->hset($key, 'k2', 'v2');
        $redis->hset($key, 'k3', 'v3');

        $r = $redis->hget($key, 'k1');
        $this->assertEquals('v1', $r);
        $r = $redis->hget($key, 'k2');
        $this->assertEquals('v2', $r);
        $r = $redis->hget($key, 'k3');
        $this->assertEquals('v3', $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHSet($redis)
    {
        $key = '?hset?';

        $redis->del($key);

        $r = $redis->hset($key, 'k1', 'v1');
        $this->assertEquals(1, $r);
        $this->assertEquals('v1', $redis->hget($key, 'k1'));
        $r = $redis->hset($key, 'k1', 'v0');
        $this->assertEquals(0, $r);
        $this->assertEquals('v0', $redis->hget($key, 'k1'));
        $r = $redis->hset($key, 'k2', 'v2');
        $this->assertEquals(1, $r);
        $this->assertEquals('v2', $redis->hget($key, 'k2'));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHSetNx($redis)
    {
        $key = '?hsetnx?';

        $redis->del($key);

        $r = $redis->hsetnx($key, 'k1', 'v1');
        $this->assertEquals(1, $r);
        $this->assertEquals('v1', $redis->hget($key, 'k1'));
        $r = $redis->hsetnx($key, 'k1', 'v0');
        $this->assertEquals(0, $r);
        $this->assertEquals('v1', $redis->hget($key, 'k1'));
        $r = $redis->hsetnx($key, 'k2', 'v2');
        $this->assertEquals(1, $r);
        $this->assertEquals('v2', $redis->hget($key, 'k2'));

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHDel($redis)
    {
        $key = '?hdel?';

        $redis->del($key);

        $r = $redis->hdel($key, 'k1', 'k2');
        $this->assertEquals(0, $r);

        $redis->hset($key, 'k1', 'v1');
        $redis->hset($key, 'k2', 'v2');
        $redis->hset($key, 'k3', 'v3');
        $redis->hset($key, 'k4', 'v4');

        $r = $redis->hdel($key, 'k1');
        $this->assertEquals(1, $r);
        $r = $redis->hdel($key, ['k1', 'k2']);
        $this->assertEquals(1, $r);
        $r = $redis->hdel($key, 'k1', 'k2', 'k3');
        $this->assertEquals(1, $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHMGet($redis)
    {
        $key = '?hmget?';

        $redis->del($key);

        $r = $redis->hmget($key, 'k1');
        $this->assertListEquals([null], $r);

        $redis->hset($key, 'k1', 'v1');
        $redis->hset($key, 'k2', 'v2');
        $redis->hset($key, 'k3', 'v3');

        $r = $redis->hmget($key, 'k1', 'k2');
        $this->assertListEquals(['v1', 'v2'], $r);
        $r = $redis->hmget($key, ['k1', 'k2']);
        $this->assertListEquals(['v1', 'v2'], $r);
        $r = $redis->hmget($key, 'k3', 'k4');
        $this->assertListEquals(['v3', null], $r);

        $redis->del($key);
    }

    /**
     * @dataProvider redis
     * @param $redis
     */
    public function testHMSet($redis)
    {
        $key = '?hmset?';

        $redis->del($key);

        $r = $redis->hmset($key, [
            'k1' => 'v1',
            'k2' => 'v2',
            'k3' => 'v3',
        ]);

        $this->assertTrue((bool) $r);

        $r = $redis->hget($key, 'k1');
        $this->assertEquals('v1', $r);
        $r = $redis->hget($key, 'k2');
        $this->assertEquals('v2', $r);
        $r = $redis->hget($key, 'k3');
        $this->assertEquals('v3', $r);

        $redis->del($key);
    }

    protected function assertListEquals($a1, $a2)
    {
        $this->assertEquals(count($a1), count($a2));
        foreach ($a1 as $i => $v1) {
            $this->assertEquals($v1, $a2[$i]);
        }
    }

    protected function assertSortedMapEquals($a1, $a2)
    {
        $this->assertListEquals(array_keys($a1), array_keys($a2));
        $this->assertListEquals(array_values($a1), array_values($a2));
    }

    protected function assertHashMapEquals($m1, $m2)
    {
        $this->assertEquals(count($m1), count($m2));
        foreach ($m1 as $k1 => $v1) {
            $this->assertEquals($v1, $m2[$k1]);
        }
    }

    protected function assertSetEquals($s1, $s2)
    {
        sort($s1);
        sort($s2);
        $this->assertListEquals($s1, $s2);
    }
}
