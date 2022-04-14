<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Contract\LockInterface;
use Rabbit\Base\Core\Channel;
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

    public function __invoke(Closure $function, bool $next = true, string $name = '', float $timeout = 600)
    {
        $name = "lock:" . (empty($name) ? uniqid() : $name);
        if (!($this->channel[$name] ?? false)) {
            $channel = new Channel();
            $this->channel[$name] = $channel;
        } else {
            $channel = $this->channel[$name];
        }
        try {
            if ($channel->isFull() && !$next) {
                return false;
            }
            if ($channel->push(1, $timeout)) {
                $nx = $timeout > 0 ? ['NX', 'EX' => (int)$timeout] : ['NX'];
                if ($this->redis->set($name, true, $nx) === null) {
                    if ($next) {
                        $this->redis->brpop("{$name}_list", (int)$timeout);
                    } else {
                        return false;
                    }
                }
            }
            return $function();
        } catch (Throwable $throwable) {
            App::error(ExceptionHelper::dumpExceptionToString($throwable));
        } finally {
            $this->redis->del($name);
            $this->redis->rpush("{$name}_list", $name);
            if ($channel->isFull()) {
                $channel->pop();
            }
        }
    }
}
