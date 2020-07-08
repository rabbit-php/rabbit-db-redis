<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Pool;

use Rabbit\Pool\PoolProperties;

/**
 * Class RedisPoolConfig
 * @package Rabbit\DB\Redis\Pool
 */
class RedisPoolConfig extends PoolProperties
{
    /**
     * @var int
     */
    protected int $db = 0;

    /**
     * @var string
     */
    protected string $prefix = '';

    /**
     * @var bool
     */
    protected bool $isSerialize = false;

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
