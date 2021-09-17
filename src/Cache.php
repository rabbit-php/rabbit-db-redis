<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Psr\SimpleCache\CacheInterface;
use Rabbit\Cache\AbstractCache;
use Throwable;

/**
 * Class Cache
 * @package rabbit\db\redis
 */
class Cache extends AbstractCache implements CacheInterface
{
    private ?Redis $client;

    public function __construct()
    {
        parent::__construct();
        $this->client = getDI('redis')->get();
    }

    public function get($key, $default = null)
    {
        $key = $this->buildKey($key);
        $result = $this->client->executeCommand('GET', [$key]);
        if ($result === false || $result === null) {
            return $default;
        }

        return $result;
    }

    public function set($key, $value, $ttl = null)
    {
        $key = $this->buildKey($key);
        if ($ttl === null) {
            return (bool)$this->client->executeCommand('SET', [$key, $value]);
        } else {
            $ttl = (int)($ttl * 1000);

            return (bool)$this->client->executeCommand('SET', [$key, $value, 'PX', $ttl]);
        }
    }

    public function delete($key)
    {
        $this->buildKey($key);
        return (bool)$this->client->executeCommand('DEL', [$key]);
    }

    public function clear()
    {
        return $this->client->executeCommand('FLUSHDB');
    }

    public function getMultiple($keys, $default = null)
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        $response = $this->client->executeCommand('MGET', $newKeys);
        $result = [];
        $i = 0;
        foreach ($keys as $key) {
            $result[$key] = $response[$i++];
        }

        return $result;
    }

    public function setMultiple($values, $ttl = null)
    {
        $args = [];
        foreach ($values as $key => $value) {
            $args[] = $this->buildKey($key);
            $args[] = $value;
        }

        $failedKeys = [];
        if ($ttl == 0) {
            $this->client->executeCommand('MSET', $args);
        } else {
            $ttl = (int)($ttl * 1000);
            $this->client->executeCommand('MULTI');
            $this->client->executeCommand('MSET', $args);
            $index = [];
            foreach ($values as $key => $value) {
                $this->client->executeCommand('PEXPIRE', [$key, $ttl]);
                $index[] = $key;
            }
            $result = $this->client->executeCommand('EXEC');
            array_shift($result);
            foreach ($result as $i => $r) {
                if ($r != 1) {
                    $failedKeys[] = $index[$i];
                }
            }
        }

        // FIXME: Where do we access failed keys from ?
        return count($failedKeys) === 0;
    }

    public function deleteMultiple($keys)
    {
        $newKeys = [];
        foreach ($keys as $key) {
            $newKeys[] = $this->buildKey($key);
        }
        return (bool)$this->client->executeCommand('DEL', $newKeys);
    }

    public function has($key)
    {
        return (bool)$this->client->executeCommand('EXISTS', [$this->buildKey($key)]);
    }
}
