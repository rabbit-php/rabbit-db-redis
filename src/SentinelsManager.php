<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Exception;
use Throwable;
use Rabbit\Base\App;

/**
 * Class SentinelsManager
 * @package rabbit\db\redis
 */
class SentinelsManager
{
    const LOG_KEY = 'redis';
    protected  $channel;
    /** @var int */
    protected int $current = 0;

    /**
     * SentinelsManager constructor.
     */
    public function __construct(int $size = 3)
    {
        $this->channel = makeChannel($size);
    }

    /**
     * @param array $sentinels
     * @param string $type
     * @param string $masterName
     * @return mixed
     * @throws Throwable
     */
    public function discover(array $sentinels, string $type, string $masterName = 'mymaster')
    {
        $size = count($sentinels);
        // $this->channel->capacity = $size;
        foreach ($sentinels as $sentinel) {
            if (is_scalar($sentinel)) {
                $sentinel = [
                    'hostname' => $sentinel
                ];
            }
            $key = $sentinel['hostname'] . (isset($sentinel['port']) ? ':' . $sentinel['port'] : '');

            if ($this->current >= $size) {
                waitChannel($this->channel, 3, 0);
                $connection = $this->channel->pop();
            } else {
                try {
                    $this->current++;
                    $connection = new SentinelConnection();
                    $connection->hostname = isset($sentinel['hostname']) ? $sentinel['hostname'] : 'localhost';
                    $connection->masterName = $masterName;
                    if (isset($sentinel['port'])) {
                        $connection->port = $sentinel['port'];
                    }
                    $connection->connectionTimeout = isset($sentinel['connectionTimeout']) ? $sentinel['connectionTimeout'] : null;
                    $connection->unixSocket = isset($sentinel['unixSocket']) ? $sentinel['unixSocket'] : null;
                } catch (Throwable $exception) {
                    $this->current--;
                    throw new Exception("can not open sentinel, $key");
                }
            }
            $method = 'get' . ucfirst($type);
            try {
                $res = $connection->$method();
            } finally {
                if ($this->channel->length() < $size) {
                    $this->channel->push($connection);
                } else {
                    $this->current--;
                }
            }
            $r = [];
            if (!is_array($res[0])) {
                $res = [$res];
            }
            foreach ($res as $index => $item) {
                for ($i = 0; $i < count($item); $i += 2) {
                    $r[$index][$item[$i]] = $item[$i + 1];
                }
            }
            $r = $r[array_rand($r)];
            if (isset($sentinel['hostname'])) {
                $connectionName = "{$connection->hostname}:{$connection->port}";
            } else {
                $connectionName = $connection->unixSocket;
            }
            if ($r) {
                App::info("Sentinel @{$connectionName} gave $type addr: {$r['ip']}:{$r['port']}", self::LOG_KEY);
                return [$r['ip'], (int)$r['port']];
            } else {
                App::error("Did not get any master from sentinel @{$connectionName}", self::LOG_KEY);
            }
        }
        throw new Exception("Master could not be discovered");
    }
}
