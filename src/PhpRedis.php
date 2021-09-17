<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Rabbit\Base\App;
use RedisClusterException;
use Rabbit\Pool\PoolManager;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Pool\AbstractConnection;

/**
 * Class PhpRedis
 * @package rabbit\db\redis
 */
class PhpRedis extends AbstractConnection
{
    use ClusterTrait;

    private ?\Redis $conn;
    private ?\RedisCluster $connCluster;
    private ?\RedisSentinel $sentinel;

    /**
     * @throws RedisClusterException|Exception
     */
    public function createConnection(): void
    {
        $pool = PoolManager::getPool($this->poolKey);
        $address = $pool->getServiceList(true);
        $parseAry = [];
        $config = $this->parseUri(current($address), $parseAry);
        if (isset($config['cluster'])) {
            $this->cluster = true;
            $this->connCluster = new \RedisCluster(NULL, $address, $pool->getTimeout(), $pool->getTimeout(), false, ArrayHelper::getValue($config, 'parameters.password', ""));
            if (isset($config['separate'])) {
                $this->conn->setOption(\RedisCluster::OPT_SLAVE_FAILOVER, \RedisCluster::FAILOVER_DISTRIBUTE_SLAVES);
            }
        } elseif (isset($config['sentinel']) && $config['sentinel']) {
            if (!$this->sentinel) {
                $this->sentinel = new \RedisSentinel($parseAry['host'], $parseAry['port'], $pool->getTimeout());
            }
            $this->conn = new \Redis();
            $retries = $pool->getPoolConfig()->getMaxRetry();
            $retries = $retries > 0 ? $retries : 1;
            while ($retries--) {
                if (false !== $master = $this->sentinel->getMasterAddrByName(ArrayHelper::getValue($config, 'master', 'mymaster'))) {
                    [$host, $port] = $master;
                    $this->conn->connect($host, (int)$port, $pool->getTimeout());

                    return;
                }
                $retries > 0 && usleep($pool->getTimeout() * 1000);
            }
            throw new Exception("Connect to redis failed!");
        } else {
            $this->conn = new \Redis();
            $this->conn->connect($parseAry['host'], (int)$parseAry['port'], $pool->getTimeout());
        }
        if ($this->conn) {
            if ($pass = ArrayHelper::getValue($config, 'parameters.password')) {
                $this->conn->auth($pass);
            }
            if ($db = ArrayHelper::getValue($config, 'parameters.database')) {
                $this->conn->select((int)$db);
            }
        }
    }

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

    public function __call($name, $arguments)
    {
        return $this->executeCommand($name, $arguments);
    }

    public function executeCommand(string $name, array $args = []): null|array|string|float|int|bool
    {
        $retries = $this->getPool()->getPoolConfig()->getMaxRetry();
        $retries = $retries > 0 ? $retries : 1;
        while ($retries--) {
            try {
                if ($this->cluster) {
                    return $this->connCluster->$name(...$args);
                }
                switch (strtolower($name)) {
                    case 'hmset':
                        $key = array_shift($args);
                        $args = Redis::parseData($args);
                        $data = $this->conn->$name($key, $args);
                        break;
                    case 'lrem':
                        $key = array_shift($args);
                        $data = $this->conn->$name($key, array_reverse($args));
                        break;
                    case 'eval':
                        $data = $this->conn->$name(array_shift($args), $args);
                        break;
                    default:
                        $data = $this->conn->$name(...$args);
                }
                return $data;
            } catch (\RedisException $e) {
                try {
                    $this->conn->ping('');
                } catch (\RedisException $ex) {
                    if ($retries === 0) {
                        throw $e;
                    }
                    App::warning(sprintf('Redis connection retry after %.3f', $this->retryDelay));
                    usleep($this->retryDelay * 1000);
                    $this->conn = null;
                    $this->createConnection();
                }
            }
        }
    }

    public function reconnect(): void
    {
        $this->createConnection();
    }
}
