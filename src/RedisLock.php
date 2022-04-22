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
            try {
                if ((int)$this->redis->setnx($name, $name) === 0) {
                    $this->redis->brpop("{$name}_list", (int)$timeout);
                }
                $function();
            } catch (Throwable $throwable) {
                App::error(ExceptionHelper::dumpExceptionToString($throwable));
            } finally {
                $this->redis->rpush("{$name}_list", $name);
            }
        }, $next, (int)$timeout);
    }
}
