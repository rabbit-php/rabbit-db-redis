<?php
declare(strict_types=1);

namespace rabbit\db\redis;


use Co\System;
use rabbit\db\Exception;
use rabbit\helper\ArrayHelper;
use rabbit\pool\AbstractConnection;
use rabbit\pool\PoolManager;

/**
 * Class PhpRedis
 * @package rabbit\db\redis
 */
class PhpRedis extends AbstractConnection
{
    use ClusterTrait;

    /** @var \RedisCluster|\Redis */
    private $conn;
    /** @var \RedisSentinel */
    private $sentinel;

    /**
     * @throws \rabbit\core\Exception
     */
    public function createConnection(): void
    {
        $pool = PoolManager::getPool($this->poolKey);
        $address = $pool->getServiceList(true);
        $parseAry = [];
        $config = $this->parseUri(current($address), $parseAry);
        $client = ArrayHelper::remove($config, 'client', 'predis');
        if (isset($config['cluster'])) {
            $this->cluster = true;
            $this->conn = new \RedisCluster(NULL, $address, $pool->getTimeout(), $pool->getTimeout(), false, ArrayHelper::getValue($config, 'parameters.password', ""));
            if (isset($config['separate'])) {
                $this->conn->setOption(\RedisCluster::OPT_SLAVE_FAILOVER, \RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
            }
        } elseif (isset($config['sentinel']) && $config['sentinel']) {
            if (!$this->sentinel) {
                $this->sentinel = new \RedisSentinel($parseAry['host'], $parseAry['port'], $pool->getTimeout());
            }
            $this->conn = new \Redis();
            $retrys = $pool->getPoolConfig()->getMaxRetry();
            $retrys = $retrys > 0 ? $retrys : 1;
            while ($retrys--) {
                if (false !== $master = $this->sentinel->getMasterAddrByName(ArrayHelper::getValue($config, 'master', 'mymaster'))) {
                    [$host, $port] = $master;
                    $this->conn->connect($host, (int)$port);
                    return;
                }
                $retry > 0 && System::sleep($pool->getTimeout());
            }
            throw new Exception("Connect to $host:$port failed!");
        } else {
            $this->conn = new \Redis();
            $retry = $pool->getPoolConfig()->getMaxRetry();
            $this->conn->connect($parseAry['host'], (int)$parseAry['port'], $pool->getTimeout());
        }
    }

    /**
     * @param string $uri
     * @return array
     * @throws \rabbit\core\Exception
     */
    protected function parseUri(string $uri, array &$parseAry): array
    {
        $parseAry = parse_url($uri);
        if (!isset($parseAry['host']) || !isset($parseAry['port'])) {
            $error = sprintf(
                'Redis Connection format is incorrect uri=%s, eg:tcp://127.0.0.1:6379/1?password=password',
                $uri
            );
            throw new Exception($error);
        }
        $query = $parseAry['query'] ?? '';
        parse_str($query, $options);
        $options['parameters']['password'] = ArrayHelper::remove($options, 'password', "");
        (isset($parseAry['path']) && !isset($options['cluster'])) && $options['parameters']['database'] = str_replace('/', '', $parseAry['path']);
        return $options;
    }

    /**
     * @param string $name
     * @param array $args
     * @return mixed
     */
    public function executeCommand(string $name, array $args = [])
    {
        return $this->conn->$name(...$args);
    }

    public function reconnect(): void
    {
        $this->createConnection();
    }
}