<?php

namespace rabbit\db\redis;

use rabbit\contract\ResultInterface;
use rabbit\db\redis\pool\RedisPool;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\PoolInterface;

/**
 * Class SwooleRedis
 * @package rabbit\redis
 */
class SwooleRedis
{
    /**
     * @var PoolInterface
     */
    protected $pool = '';

    /**
     * Redis constructor.
     * @param RedisPool $pool
     */
    public function __construct(RedisPool $pool)
    {
        $this->pool = $pool;
    }

    /**
     * @return ConnectionInterface
     */
    public function getConn(): ConnectionInterface
    {
        return $this->pool->getConnection();
    }

    /**
     * @param string $method
     * @param array $params
     * @return ResultInterface
     * @throws \Exception
     */
    public function deferCall(string $method, array $params)
    {
        /* @var $client Connection */
        $client = $this->pool->getConnection();
        $client->setDefer();
        $result = $client->$method(...$params);

        return $this->getResult($client, $result);
    }

    /**
     * @param ConnectionInterface $connection
     * @param $result
     * @return ResultInterface
     */
    private function getResult(ConnectionInterface $connection, $result): ResultInterface
    {
        return new RedisResult($connection, $result);
    }

    /**
     * @param $method
     * @param $arguments
     * @return mixed
     */
    public function __call($method, $arguments)
    {
        return $this->call($method, $arguments);
    }

    /**
     * @param string $method
     * @param array $params
     * @return mixed
     */
    public function call(string $method, array $params)
    {
        /* @var Connection $client */
        $client = $this->pool->getConnection();
        $result = $client->$method(...$params);
        $client->release(true);

        return $result;
    }

    /**
     * @param array $config
     * @return array
     * @throws \Exception
     */
    public static function getCurrent(array $config): array
    {
        if (isset($config['sentinel']) && (int)$config['sentinel'] === 1) {
            $sentinels = [];
            if (filter_var($config['host'], FILTER_VALIDATE_IP)) {
                $sentinels[] = array_filter([
                    'hostname' => $config['host'],
                    'port' => $config['port']
                ]);
            } else {
                $res = \Co::getaddrinfo($config['host']);
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
            return getDI(SentinelsManager::class)->discoverMaster($sentinels, isset($config['master']) ? $config['master'] : 'mymaster');
        }
        $host = $config['host'];
        $port = (int)$config['port'];
        return [$host, $port];
    }
}
