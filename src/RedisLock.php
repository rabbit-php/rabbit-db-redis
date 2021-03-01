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
class RedisLock implements LockInterface
{
    /** @var Redis */
    protected ?Redis $redis;

    /**
     * RedisLock constructor.
     * @param Redis|null $redis
     * @throws Throwable
     */
    public function __construct(Redis $redis = null)
    {
        $this->redis = $redis ?? getDI('redis')->get();
    }

    /**
     * @param Closure $function
     * @param string $name
     * @param float|int $timeout
     * @return bool|mixed
     * @throws Throwable
     */
    public function __invoke(Closure $function, bool $next = true, string $name = '', float $timeout = 600)
    {
        $name = "lock." . (empty($name) ? uniqid() : $name);
        try {
            $nx = $timeout > 0 ? ['NX', 'EX' => $timeout] : ['NX'];
            while ($this->redis->set($name, true, $nx) === null) {
                if ($next) {
                    usleep(10 * 1000);
                } else {
                    return false;
                }
            }
            $result = $function();
            $this->redis->del($name);
            return $result;
        } catch (Throwable $throwable) {
            App::error(ExceptionHelper::dumpExceptionToString($throwable));
            $this->redis->del($name);
        }
    }
}
