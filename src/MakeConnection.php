<?php
declare(strict_types=1);

namespace rabbit\db\redis;

use rabbit\core\ObjectFactory;
use rabbit\helper\ArrayHelper;
use rabbit\db\redis\pool\RedisPool;
use rabbit\db\redis\pool\RedisPoolConfig;

/**
 * Class MakeConnection
 * @package rabbit\db\redis
 */
class MakeConnection
{
    /**
     * @param string $name
     * @param string $dsn
     * @param array $pool
     * @param array $config
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(
        string $class,
        string $name,
        string $dsn,
        array $pool = []
    ): void
    {
        /** @var Manager $manager */
        $manager = getDI('redis.manager');
        if (!$manager->hasConnection($name)) {
            [
                $min,
                $max,
                $wait,
                $retry
            ] = ArrayHelper::getValueByArray($pool, ['min', 'max', 'wait', 'retry'], null, [
                10,
                10,
                0,
                3
            ]);
            $conn = [
                $name => [
                    'class' => str_replace('Connection', 'Redis', $class),
                    'pool' => ObjectFactory::createObject([
                        'class' => RedisPool::class,
                        'connection' => $class,
                        'poolConfig' => ObjectFactory::createObject([
                            'class' => RedisPoolConfig::class,
                            'minActive' => intval($min / swoole_cpu_num()),
                            'maxActive' => intval($max / swoole_cpu_num()),
                            'maxWait' => $wait,
                            'maxReconnect' => $retry,
                            'uri' => [$dsn]
                        ], [], false)
                    ], [], false)
                ]
            ];
            $manager->addConnection($conn);
        }
    }
}