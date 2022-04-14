<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Contract\LockInterface;
use Rabbit\Base\Helper\ExceptionHelper;
use Throwable;

/**
 * Class RedisLock
 * @package Rabbit\DB\Redis
 */
final class RedisLock implements LockInterface
{
    protected readonly ?Redis $redis;

    public function __construct(Redis $redis = null)
    {
        $this->redis = $redis ?? service('redis')->get();
    }

    public function __invoke(Closure $function, bool $next = true, string $name = '', float $timeout = 600)
    {
        $name = "lock:" . (empty($name) ? uniqid() : $name);
        try {
            $nx = $timeout > 0 ? ['NX', 'EX' => (int)$timeout] : ['NX'];
            if ($this->redis->set($name, true, $nx) === null) {
                if ($next) {
                    $this->redis->brpop("{$name}_list", (int)$timeout);
                } else {
                    return false;
                }
            }
            return $function();
        } catch (Throwable $throwable) {
            App::error(ExceptionHelper::dumpExceptionToString($throwable));
        } finally {
            $this->redis->del($name);
            $this->redis->rpush("{$name}_list", $name);
        }
    }
}
