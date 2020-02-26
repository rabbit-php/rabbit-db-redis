<?php

namespace rabbit\db\redis;

use rabbit\App;
use rabbit\core\Exception;
use rabbit\pool\AbstractConnection;
use rabbit\pool\PoolManager;
use rabbit\redis\pool\RedisPoolConfig;
use Swoole\Coroutine\System;

/**
 * Class SwooleConnection
 * @package rabbit\db\redis
 */
class SwooleConnection extends AbstractConnection
{
    /**
     * @var \Swoole\Coroutine\Redis
     */
    private $connection;

    /**
     *
     */
    public function reconnect(): void
    {
        $this->createConnection();
    }

    /**
     *
     */
    public function createConnection(): void
    {
        $this->connection = $this->initRedis();
    }

    /**
     * @return \Swoole\Coroutine\Redis
     * @throws Exception
     */
    protected function initRedis(): \Swoole\Coroutine\Redis
    {
        $pool = PoolManager::getPool($this->poolKey);
        $timeout = $pool->getTimeout();
        $address = $pool->getConnectionAddress();
        $config = $this->parseUri($address);
        $options = $pool->getPoolConfig()->getOptions();

        [$host, $port] = Redis::getCurrent($config);

        $db = isset($config['db']) && (0 <= $config['db'] && $config['db'] <= 16) ? intval($config['db']) : 0;
        $password = isset($config['password']) ? $config['password'] : null;
        $redis = $this->getConnectRedis($host, $port, $db, $timeout, $password);
        if ($options) {
            $redis->setOptions($options);
        }
        return $redis;
    }

    /**
     * @param string $uri
     * @return array
     * @throws Exception
     */
    protected function parseUri(string $uri): array
    {
        $parseAry = parse_url($uri);
        if (!isset($parseAry['host']) || !isset($parseAry['port'])) {
            $error = sprintf(
                'Redis Connection format is incorrect uri=%s, eg:tcp://127.0.0.1:6379/1?password=password',
                $uri
            );
            throw new Exception($error);
        }
        isset($parseAry['path']) && $parseAry['db'] = str_replace('/', '', $parseAry['path']);
        $query = $parseAry['query'] ?? '';
        parse_str($query, $options);
        $configs = array_merge($parseAry, $options);
        unset($configs['path']);
        unset($configs['query']);

        return $configs;
    }

    /**
     * @param string $host
     * @param int $port
     * @param int $db
     * @param float $timeout
     * @param string|null $password
     * @return \Swoole\Coroutine\Redis
     * @throws Exception
     */
    protected function getConnectRedis(
        string $host,
        int $port,
        int $db,
        float $timeout,
        string $password = null
    ): \Swoole\Coroutine\Redis
    {
        /* @var RedisPoolConfig $poolConfig */
        $poolConfig = PoolManager::getPool($this->poolKey)->getPoolConfig();
        $serialize = $poolConfig->getSerialize();
        $redis = new \Swoole\Coroutine\Redis(['timeout' => $timeout]);
        $retry = $this->retries;
        while ($retry-- > 0) {
            $result = $redis->connect($host, $port, $serialize);
            if ($result !== false) {
                if ($password) {
                    $redis->auth($password);
                }
                $redis->select($db);

                return $redis;
            }
            App::warning(sprintf('Redis connection retry host=%s port=%d,after %.3f', $host, $port));
            System::sleep($this->retryDelay);
        }
        $error = sprintf('Redis connection failure host=%s port=%d', $host, $port);
        throw new Exception($error);
    }

    /**
     * @return bool
     */
    public function check(): bool
    {
        try {
            if (false === $this->connection->ping()) {
                throw new \RuntimeException('Connection lost');
            }
            $connected = true;
        } catch (\Throwable $throwable) {
            $connected = false;
        }

        return $connected;
    }

    /**
     * @param float|null $timeout
     * @return mixed
     */
    public function receive(float $timeout = -1)
    {
        $result = $this->connection->recv($timeout);
        $this->connection->setDefer(false);
        $this->recv = true;

        return $result;
    }

    /**
     * @param bool $defer
     */
    public function setDefer($defer = true): bool
    {
        $this->recv = false;
        return $this->connection->setDefer($defer);
    }

    /**
     * @param $method
     * @param $arguments
     * @return mixed
     */
    public function __call($method, $arguments)
    {
        return $this->connection->$method(...$arguments);
    }

    /**
     * @param string $name
     * @param array $params
     * @return mixed
     */
    public function executeCommand(string $name, array $params = [])
    {
        return $this->connection->$name(...$params);
    }
}
