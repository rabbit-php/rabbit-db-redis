<?php


namespace rabbit\db\redis\pool;

use rabbit\pool\ConnectionInterface;
use rabbit\pool\ConnectionPool;
use rabbit\redis\Connection;

/**
 * Class RedisPool
 * @package rabbit\db\redis\pool
 */
class RedisPool extends ConnectionPool
{
    /**
     * @var Connection|string
     */
    protected $connection;

    /**
     * @return ConnectionInterface
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function createConnection(): ConnectionInterface
    {
        $connection = $this->connection;
        $redis = is_string($this->connection) ? new $connection($this->getPoolConfig()->getName()) : new Connection($this->getPoolConfig()->getName());
        return $redis;
    }
}
