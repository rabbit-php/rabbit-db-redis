<?php

namespace rabbit\db\redis;

use rabbit\App;
use rabbit\core\Exception;
use rabbit\helper\ArrayHelper;
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
    use ClusterTrait;

    /**
     * @var \Swoole\Coroutine\Redis
     */
    private $master;
    /**
     * @var \Swoole\Coroutine\Redis
     */
    private $slave;
    /** @var string */
    private $currConn = SwooleRedis::CONN_MASTER;

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
        $pool = PoolManager::getPool($this->poolKey);
        $timeout = $pool->getTimeout();
        $address = $pool->getConnectionAddress();
        $config = $this->parseUri($address);
        $options = $pool->getPoolConfig()->getOptions();
        if ($this->currConn === SwooleRedis::CONN_MASTER) {
            $this->makeConn($config, $options, SwooleRedis::CONN_MASTER);
        }
        if (ArrayHelper::remove($config, 'separate', false) || $this->currConn === SwooleRedis::CONN_SLAVE) {
            $this->makeConn($config, SwooleRedis::CONN_SLAVE);
        }
    }

    /**
     * @param array $config
     * @param array $options
     * @param string $type
     * @throws Exception
     */
    private function makeConn(array $config, array $options, string $type): void
    {
        [$host, $port] = Redis::getCurrent($config, $type);

        $db = isset($config['db']) && (0 <= $config['db'] && $config['db'] <= 16) ? intval($config['db']) : 0;
        $password = isset($config['password']) ? $config['password'] : null;
        $redis = $this->getConnectRedis($host, $port, $db, $password);
        if ($options) {
            $redis->setOptions($options);
        }
        $this->$type = $redis;
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
        string $password = null
    ): \Swoole\Coroutine\Redis
    {
        /* @var RedisPoolConfig $poolConfig */
        $poolConfig = PoolManager::getPool($this->poolKey)->getPoolConfig();
        $serialize = $poolConfig->getSerialize();
        $redis = new \Swoole\Coroutine\Redis(['timeout' => $poolConfig->getTimeout()]);
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
            App::warning(sprintf('Redis connection retry host=%s port=%d,after %.3f', $host, $port, $this->retryDelay));
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
            if (false === $this->master->ping()) {
                $this->currConn = SwooleRedis::CONN_MASTER;
                App::warning('Master Connection lost');
                return false;
            }
            if ($this->slave && false === $this->slave->ping()) {
                $this->currConn = SwooleRedis::CONN_SLAVE;
                App::warning('Slave Connection lost');
                return false;
            }
            return true;
        } catch (\Throwable $throwable) {
            return false;
        }
    }

    /**
     * @param float|null $timeout
     * @return mixed
     */
    public function receive(float $timeout = -1)
    {
        $result = $this->{$this->currConn}->recv($timeout);
        $this->{$this->currConn}->setDefer(false);
        $this->recv = true;

        return $result;
    }

    /**
     * @param bool $defer
     */
    public function setDefer($defer = true): bool
    {
        $this->recv = false;
        return $this->{$this->currConn}->setDefer($defer);
    }

    /**
     * @param $method
     * @param $arguments
     * @return mixed
     */
    public function __call($method, $arguments)
    {
        $this->separate();
        return $this->{$this->currConn}->$method(...$arguments);
    }

    /**
     * @param string $name
     * @param array $params
     * @return mixed
     */
    public function executeCommand(string $name, array $params = [])
    {
        $this->separate();
        return $this->{$this->currConn}->$name(...$params);
    }

    private function separate(): void
    {
        if ($this->slave && in_array($method, SwooleRedis::READ_COMMAND)) {
            $this->currConn = SwooleRedis::CONN_SLAVE;
        } else {
            $this->currConn = SwooleRedis::CONN_MASTER;
        }
    }
}
