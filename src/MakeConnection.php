<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;


use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Redis\Pool\RedisPool;
use Rabbit\DB\Redis\Pool\RedisPoolConfig;
use Rabbit\Pool\BaseManager;
use Throwable;

/**
 * Class MakeConnection
 * @package Rabbit\DB\Redis
 */
class MakeConnection
{
    /**
     * @param string $class
     * @param string $name
     * @param string $dsn
     * @param array $pool
     * @throws Throwable
     */
    public static function addConnection(
        string $class,
        string $name,
        string $dsn,
        array $pool = []
    ): void
    {
        /** @var BaseManager $manager */
        $manager = service('redis');
        if (!$manager->has($name)) {
            [
                $min,
                $max,
                $wait,
                $retry
            ] = ArrayHelper::getValueByArray($pool, ['min', 'max', 'wait', 'retry'], [
                10,
                10,
                0,
                3
            ]);
            $conn = [
                $name => [
                    '{}' => str_replace('Connection', 'Redis', $class),
                    'pool' => create([
                        '{}' => RedisPool::class,
                        'connection' => $class,
                        'poolConfig' => create([
                            '{}' => RedisPoolConfig::class,
                            'minActive' => intval($min),
                            'maxActive' => intval($max),
                            'maxWait' => $wait,
                            'maxRetry' => $retry,
                            'uri' => [$dsn]
                        ], [], false)
                    ], [], false)
                ]
            ];
            $manager->add($conn);
        }
    }
}