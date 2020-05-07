<?php
declare(strict_types=1);

namespace rabbit\db\redis;

use Predis\Client;
use rabbit\db\Exception;
use rabbit\exception\NotSupportedException;
use rabbit\helper\ArrayHelper;
use rabbit\pool\AbstractConnection;
use rabbit\pool\PoolManager;

/**
 * Class Predis
 * @package rabbit\db\redis
 */
class Predis extends AbstractConnection
{
    /** @var Client */
    private $conn;

    /**
     * @throws \rabbit\core\Exception
     */
    public function createConnection(): void
    {
        $pool = PoolManager::getPool($this->poolKey);
        $address = $pool->getServiceList(true);
        $config = $this->parseUri(current($address));
        $this->conn = new Client($address, $config);
    }

    /**
     * @param string $uri
     * @return array
     * @throws \rabbit\core\Exception
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
        (isset($parseAry['path']) && !isset($options['cluster'])) && $options['parameters']['database'] = str_replace('/', '', $parseAry['path']);
        return $options;
    }

    /**
     * @param string $name
     * @param array $params
     */
    public function executeCommand(string $name, array $params = [])
    {
        return $this->conn->$name(...$params);
    }

    public function reconnect(): void
    {
        $this->createConnection();
    }

    public function check(): bool
    {
        return true;
    }

    public function receive(float $timeout = -1)
    {
        throw new NotSupportedException('socket connection not support ' . __METHOD__);
    }

}