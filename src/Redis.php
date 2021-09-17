<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Throwable;
use Rabbit\Base\App;
use Rabbit\Pool\PoolInterface;
use Rabbit\Pool\ConnectionInterface;
use Rabbit\Base\Exception\NotSupportedException;

/**
 * Class Redis
 * @package Rabbit\DB\Redis
 */
class Redis implements ConnectionInterface
{
    const CONN_MASTER = 'master';
    const CONN_SLAVE = 'slave';
    protected PoolInterface $pool;

    const READ_COMMAND = [
        'DBSIZE',
        'INFO',
        'MONITOR',
        'EXISTS',
        'TYPE',
        'KEYS',
        'SCAN',
        'RANDOMKEY',
        'TTL',
        'GET',
        'MGET',
        'SUBSTR',
        'STRLEN',
        'GETRANGE',
        'GETBIT',
        'LLEN',
        'LRANGE',
        'LINDEX',
        'SCARD',
        'SISMEMBER',
        'SINTER',
        'SUNION',
        'SDIFF',
        'SMEMBERS',
        'SSCAN',
        'SRANDMEMBER',
        'ZRANGE',
        'ZREVRANGE',
        'ZRANGEBYSCORE',
        'ZREVRANGEBYSCORE',
        'ZCARD',
        'ZSCORE',
        'ZCOUNT',
        'ZRANK',
        'ZREVRANK',
        'ZSCAN',
        'HGET',
        'HMGET',
        'HEXISTS',
        'HLEN',
        'HKEYS',
        'HVALS',
        'HGETALL',
        'HSCAN',
        'PING',
        'AUTH',
        'SELECT',
        'ECHO',
        'QUIT',
        'OBJECT',
        'BITCOUNT',
        'TIME',
        'SORT',
    ];

    public function __construct(PoolInterface $pool)
    {
        $this->pool = $pool;
    }

    public function __invoke(\Closure $function)
    {
        $conn = $this->pool->get();
        try {
            return $function($conn);
        } catch (\Throwable $exception) {
            App::error($exception->getMessage(), 'redis');
            throw $exception;
        } finally {
            $conn->release(true);
        }
    }

    public function getPool(): PoolInterface
    {
        return $this->pool;
    }

    public function __call($method, $arguments)
    {
        $client = $this->pool->get();
        try {
            return $client->$method(...$arguments);
        } catch (Throwable $exception) {
            App::error($exception->getMessage(), 'redis');
            throw $exception;
        } finally {
            $client->release(true);
        }
    }

    public static function getCurrent(array $config, string $type): array
    {
        if (isset($config['sentinel']) && (int)$config['sentinel'] === 1) {
            $sentinels = [];
            if (filter_var($config['host'], FILTER_VALIDATE_IP)) {
                $sentinels[] = array_filter([
                    'hostname' => $config['host'],
                    'port' => $config['port']
                ]);
            } else {
                $res = gethostbynamel($config['host']);
                if ($res) {
                    foreach ($res as $ip) {
                        $sentinels[] = array_filter([
                            'hostname' => $ip,
                            'port' => $config['port']
                        ]);
                    }
                } else {
                    $sentinels[] = array_filter([
                        'hostname' => $config['host'],
                        'port' => $config['port']
                    ]);
                }
            }
            return create(SentinelsManager::class, ['size' => count($sentinels)])->discover($sentinels, $type, isset($config['master']) ? $config['master'] : 'mymaster');
        }
        $host = $config['host'];
        $port = (int)$config['port'];
        return [$host, $port];
    }

    public static function parseData(array $data): array
    {
        $row = [];
        $c = count($data);
        for ($i = 0; $i < $c;) {
            $row[$data[$i++]] = $data[$i++];
        }
        return $row;
    }

    public static function parseStream(array $data): array
    {
        $row = [];
        $c = count($data);
        for ($i = 0; $i < $c;) {
            $row[$data[$i++]] = is_array($child = $data[$i++]) ? self::parseMsg($child) : $child;
        }
        return $row;
    }

    public static function parseMsg(array $data): array
    {
        $row = [];
        foreach ($data as [$id, $msg]) {
            $row[$id] = self::parseData($msg);
        }
        return $row;
    }

    public function createConnection(): void
    {
        throw new NotSupportedException("Redis Manager not support " . __METHOD__);
    }

    public function reconnect(): void
    {
        throw new NotSupportedException("Redis Manager not support " . __METHOD__);
    }

    public function release($release = false): void
    {
        throw new NotSupportedException("Redis Manager not support " . __METHOD__);
    }
}
