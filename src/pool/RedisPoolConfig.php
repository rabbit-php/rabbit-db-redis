<?php


namespace rabbit\db\redis\pool;

use rabbit\pool\PoolProperties;

/**
 * Class RedisPoolConfig
 * @package rabbit\db\redis\pool
 */
class RedisPoolConfig extends PoolProperties
{
    /**
     * @var int
     */
    protected $db = 0;

    /**
     * @var string
     */
    protected $prefix = '';

    /**
     * @var bool
     */
    protected $isSerialize = false;

    /**
     * @return bool
     */
    public function getSerialize(): bool
    {
        return $this->isSerialize;
    }

    /**
     * @return int
     */
    public function getDb(): int
    {
        return $this->db;
    }

    /**
     * @return string
     */
    public function getPrefix(): string
    {
        return $this->prefix;
    }
}
