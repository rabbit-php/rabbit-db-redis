<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Contract\LockInterface;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Pool\AbstractConnection;
use Throwable;

final class RedisLock implements LockInterface
{
    protected Redis $redis;
    protected array $channel;

    public function __construct(Redis $redis = null)
    {
        $this->redis = $redis ?? service('redis')->get();
    }

    public function __invoke(string $name, Closure $function, bool $next = true, float $timeout = 10): void
    {
        $name = "lock:{$name}";
        $redis = $this->redis;
        lock($name, function () use ($name, $timeout, $function, $redis): void {
            $redis(function (AbstractConnection $conn) use ($name, $timeout, $function): void {
                $time = time();
                $conn->setTimeout($timeout);
                $start = 0;
                $end = 0;
                try {
                    if ((int)$this->redis->eval("local ret = redis.call('setnx',KEYS[1],ARGV[1]) 
                    redis.call('expire',KEYS[1],ARGV[2]) 
                    return ret", 1, $name, $time, $timeout) === 0) {
                        $conn->brpop("{$name}:list", (int)$timeout);
                    }
                    $start = time();
                    $function();
                } catch (Throwable $throwable) {
                    App::error(ExceptionHelper::dumpExceptionToString($throwable));
                } finally {
                    $end = time();
                    $conn->eval("if redis.call('llen',KEYS[2])==0 
                    then redis.call('rpush',KEYS[2],ARGV[1]) redis.call('expire',KEYS[2],ARGV[2]) end return 1", 2, $name, "{$name}:list", $time, (int)$timeout - ($end - $start));
                }
            });
        }, $next, (int)$timeout);
    }
}
