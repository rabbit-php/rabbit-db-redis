<?php
declare(strict_types=1);

namespace rabbit\db\redis;


use rabbit\helper\ExceptionHelper;
use rabbit\memory\atomic\LockInterface;

/**
 * Class RedisLock
 * @package rabbit\db\redis
 */
class RedisLock implements LockInterface
{
    /** @var SwooleRedis */
    protected $redis;

    /**
     * RedisLock constructor.
     * @param SwooleRedis|null $redis
     * @throws \Exception
     */
    public function __construct(SwooleRedis $redis = null)
    {
        $this->redis = $redis ?? getDI('redis');
    }

    /**
     * @param \Closure $function
     * @param string $name
     * @param float|int $timeout
     * @param array $params
     * @return bool|mixed
     * @throws \Exception
     */
    public function __invoke(\Closure $function, string $name = '', float $timeout = 600, array $params = [])
    {
        try {
            if ($this->redis->set($name, true, ['NX', 'EX' => $timeout]) === false) {
                return false;
            }
            $result = call_user_func($function, ...$params);
            return $result;
        } catch (\Throwable $throwable) {
            print_r(ExceptionHelper::convertExceptionToArray($throwable));
        } finally {
            $this->redis->del($name);
        }
    }

}