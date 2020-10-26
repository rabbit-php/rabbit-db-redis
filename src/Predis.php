<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Throwable;
use Predis\Client;
use Rabbit\Base\App;
use Rabbit\Pool\PoolManager;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Pool\AbstractConnection;

/**
 * Class Predis
 * @package rabbit\db\redis
 */
class Predis extends AbstractConnection
{
    use ClusterTrait;

    private ?Client $conn = null;

    public function createConnection(): void
    {
        $pool = PoolManager::getPool($this->poolKey);
        $address = $pool->getServiceList(true);
        $currAddr = current($address);
        $config = $this->parseUri($currAddr);
        if (isset($config['sentinel'])) {
            $config['replication'] = 'sentinel';
            $config['service'] = ArrayHelper::remove($config, 'master', 'mymaster');
            unset($config['sentinel']);
        } else {
            $address = count($address) === 1 ? $currAddr : $address;
        }
        $this->conn = new Client($address, $config);
        $this->cluster = (bool)ArrayHelper::getValue($config, 'cluster', false);
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
        $query = $parseAry['query'] ?? '';
        parse_str($query, $options);
        $options['parameters']['password'] = ArrayHelper::remove($options, 'password', null);
        (isset($parseAry['path']) && !isset($options['cluster'])) && $options['parameters']['database'] = (int)str_replace('/', '', $parseAry['path']);
        return $options;
    }

    /**
     * @param string $name
     * @param array $params
     * @return mixed
     */
    public function executeCommand(string $name, array $params = [])
    {
        $retries = $this->getPool()->getPoolConfig()->getMaxRetry();
        $retries = $retries > 0 ? $retries : 1;
        while ($retries--) {
            try {
                return $this->conn->$name(...$params);
            } catch (Throwable $e) {
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

    /**
     * @throws Throwable
     */
    public function reconnect(): void
    {
        App::warning("predis reconnecting...");
        $this->createConnection();
    }
}
