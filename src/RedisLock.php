<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Contract\LockInterface;
use Rabbit\Base\Helper\ExceptionHelper;
use Throwable;

final class RedisLock implements LockInterface
{
    protected Redis $redis;
    protected array $channel;

    public function __construct(Redis $redis = null)
    {
        $this->redis = $redis ?? service('redis')->get();
    }

    public function __invoke(string $name, Closure $function, bool $next = true, float $timeout = 600): void
    {
        $name = "lock:{$name}";
        lock($name, function () use ($name, $timeout, $function): void {
            $time = time();
            try {
                $this->redis->eval("redis.call('setnx',KEYS[1],ARGV[1]) 
                if redis.call('llen',KEYS[2])==0 
                then redis.call('RPUSH',KEYS[2],ARGV[1]) end return 1", 2, $name, "{$name}_list", $time);
                if ((int)$this->redis->setnx($name, $time) === 0) {
                    $this->redis->brpop("{$name}_list", (int)$timeout);
                }
                $function();
            } catch (Throwable $throwable) {
                App::error(ExceptionHelper::dumpExceptionToString($throwable));
            } finally {
                $this->redis->eval("if redis.call('llen',KEYS[2])==0 
                then redis.call('RPUSH',KEYS[2],ARGV[1]) end return 1", 2, $name, "{$name}_list", $time);
            }
        }, $next, (int)$timeout);
    }
}
