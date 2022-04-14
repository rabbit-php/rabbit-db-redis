<?php

declare(strict_types=1);

use Rabbit\Base\Helper\LockHelper;
use Rabbit\DB\Redis\RedisLock;

if (!function_exists('rlock')) {
    function rlock(string $key, callable $function, bool $next = true, float $timeout = 600, string $name = 'default')
    {
        if (null === $lock = LockHelper::getLock("redis:$name")) {
            $lock = new RedisLock(service('redis')->get($name));
            LockHelper::add("redis:$name", $lock);
        }
        return $lock($function, $next, $key, $timeout);
    }
}
