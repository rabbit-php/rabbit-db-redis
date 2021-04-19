<?php

declare(strict_types=1);

use Rabbit\Base\Helper\LockHelper;
use Rabbit\DB\Redis\RedisLock;

if (!function_exists('rlock')) {
    function rlock(callable $function, bool $next = true, string $key = '', float $timeout = 600, string $name = 'default')
    {
        if (null === $lock = LockHelper::getLock('redis')) {
            $lock = new RedisLock(getDI('redis')->get($name));
            LockHelper::add('redis', $lock);
        }
        return $lock($function, $next, $key, $timeout);
    }
}
