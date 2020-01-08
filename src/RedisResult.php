<?php

namespace rabbit\db\redis;

use rabbit\pool\AbstractResult;

/**
 * Class RedisResult
 * @package rabbit\db\redis
 */
class RedisResult extends AbstractResult
{
    /**
     * @param mixed ...$params
     * @return mixed
     */
    public function getResult(...$params)
    {
        return $this->recv(true);
    }
}
