<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Rabbit\Base\Helper\ExceptionHelper;
use rabbit\memory\atomic\LockInterface;
use Throwable;

/**
 * Class RedisLock
 * @package Rabbit\DB\Redis
 */
class RedisLock implements \Rabbit\Base\Contract\LockInterface
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
     * @param array $params
     * @return bool|mixed
     * @throws Throwable
     */
    public function __invoke(Closure $function, string $name = '', float $timeout = 600)
    {
        try {
            $name = empty($name) ? uniqid() : $name;
            if ($this->redis->set($name, true, ['NX', 'EX' => $timeout]) === false) {
                return false;
            }
            return $function();
        } catch (Throwable $throwable) {
            print_r(ExceptionHelper::convertExceptionToArray($throwable));
        } finally {
            $this->redis->del($name);
        }
    }

}