<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Throwable;
use Rabbit\Base\App;
use Rabbit\Pool\PoolManager;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Pool\AbstractConnection;

class Connection extends AbstractConnection
{
    use ClusterTrait;

    private string $hostname = 'localhost';

    private int $port = 6379;

    private ?string $unixSocket = null;

    private int $database = 0;

    private ?float $connectionTimeout = null;

    private ?float $dataTimeout = null;

    private ?float $dyTimeout = null;

    private int $socketClientFlags = STREAM_CLIENT_CONNECT;

    private $_socket = null;

    private $_socketSlave = null;

    private bool $separate = false;

    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    public function close(bool $quit = true)
    {
        if ($this->_socket !== null) {
            $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port) . ', database=' . $this->database;
            App::warning('Closing DB connection: ' . $connection);
            if ($quit) {
                try {
                    $this->executeCommand('QUIT');
                } catch (SocketException $e) {
                    // ignore errors when quitting a closed connection
                }
            }
            fclose($this->_socket);
            $this->_socket = null;
        }
        if ($this->_socketSlave !== null) {
            fclose($this->_socketSlave);
            $this->_socketSlave = null;
        }
    }

    public function setTimeout(float $timeout): void
    {
        $this->dyTimeout = $timeout;
    }

    public function executeCommand(string $name, array $params = []): array|bool|null|string
    {
        $name = strtoupper($name);
        $tmp = [];
        if ($this->_socketSlave && in_array($name, Redis::READ_COMMAND)) {
            $type = '_socketSlave';
        } else {
            $type = '_socket';
        }
        $this->parseParams($params, $tmp);
        $params = [...explode(' ', $name), ...$tmp];
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen((string)$arg, '8bit') . "\r\n" . $arg . "\r\n";
        }
        App::debug("Executing Redis Command: {$name}");
        $this->open();
        $retries = $this->getPool()->getPoolConfig()->getMaxRetry();
        $retries = $retries > 0 ? $retries : 1;
        while ($retries--) {
            try {
                $data = $this->sendCommandInternal($command, $params, $type);
                if (in_array($name, ['HGETALL', 'CONFIG']) && is_array($data) && !empty($data)) {
                    return Redis::parseData($data);
                } elseif (in_array($name, ['XREAD', 'XREADGROUP']) && is_array($data) && !empty($data)) {
                    return Redis::parseStream(reset($data));
                }
                return $data;
            } catch (SocketException $e) {
                if ($retries === 0) {
                    throw $e;
                }
                $this->close(false);
                App::warning(sprintf('Redis connection retry host=%s port=%d,after %.3f', $this->hostname, $this->port, $this->retryDelay));
                usleep($this->retryDelay * 1000 * 1000);
                $this->$type = null;
                $this->open();
            }
        }
    }

    private function parseParams(array $params, array &$tmp): void
    {
        foreach ($params as $index => $item) {
            if (is_array($item)) {
                $this->parseParams($item, $tmp);
            } elseif (is_int($index)) {
                $tmp[] = $item;
            } else {
                $tmp = [...$tmp, $index, $item];
            }
        }
    }

    public function open(): void
    {
        if ($this->_socket !== null) {
            return;
        }
        $pool = PoolManager::getPool($this->poolKey);
        $this->connectionTimeout = $this->dataTimeout = $pool->getTimeout();
        $address = $pool->getConnectionAddress();
        $config = $this->parseUri($address);
        $this->separate = (bool)ArrayHelper::remove($config, 'separate', false);
        $this->makeConn($config, '_socket');
        if ($this->separate && $this->_socketSlave === null) {
            $this->makeConn($config, '_socketSlave');
        }
    }

    private function makeConn(array $config, string $type): void
    {
        [$this->hostname, $this->port] = Redis::getCurrent($config, $type === '_socket' ? 'master' : 'slave');

        $this->database = isset($config['db']) && (0 <= $config['db'] && $config['db'] <= 16) ? intval($config['db']) : 0;
        $password = isset($config['password']) ? $config['password'] : null;
        $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port) . ', database=' . $this->database;
        App::debug('Opening redis DB connection: ' . $connection);
        $this->$type = @stream_socket_client(
            $this->unixSocket ? 'unix://' . $this->unixSocket : 'tcp://' . $this->hostname . ':' . $this->port,
            $errorNumber,
            $errorDescription,
            $this->connectionTimeout ? $this->connectionTimeout : ini_get('default_socket_timeout'),
            $this->socketClientFlags
        );
        if ($this->$type) {
            if ($this->dyTimeout !== null) {
                stream_set_timeout(
                    $this->$type,
                    $timeout = (int)$this->dyTimeout,
                    (int)(($this->dyTimeout - $timeout) * 1000000)
                );
            } elseif ($this->dataTimeout !== null) {
                stream_set_timeout(
                    $this->$type,
                    $timeout = (int)$this->dataTimeout,
                    (int)(($this->dataTimeout - $timeout) * 1000000)
                );
            }
            if ($password !== null) {
                $this->executeCommand('AUTH', [$password]);
            }
            if ($this->database !== null) {
                $this->executeCommand('SELECT', [$this->database]);
            }
        } else {
            $this->$type = null;
            App::error("Failed to open redis DB connection ($connection): $errorNumber - $errorDescription");
            $message = config('debug') ? "Failed to open redis DB connection ($connection): $errorNumber - $errorDescription" : 'Failed to open DB connection.';
            throw new Exception($message . ';' . $errorDescription, $errorNumber);
        }
    }

    protected function parseUri(string $uri): array
    {
        $parseAry = parse_url($uri);
        if (!isset($parseAry['host']) || !isset($parseAry['port'])) {
            $error = sprintf(
                'Redis Connection format is incorrect uri=%s, eg:tcp://127.0.0.1:6379/1?password=password',
                $uri
            );
            throw new Exception($error);
        }
        isset($parseAry['path']) && $parseAry['db'] = str_replace('/', '', $parseAry['path']);
        $query = $parseAry['query'] ?? '';
        parse_str($query, $options);
        $configs = [...$parseAry, ...$options];
        unset($configs['path']);
        unset($configs['query']);

        return $configs;
    }

    private function sendCommandInternal(string $command, array $params, string $type): array|bool|null|string
    {
        $written = @fwrite($this->$type, $command);
        if ($written === false) {
            throw new SocketException("Failed to write to socket.\nRedis command was: " . $command);
        }
        if ($written !== ($len = mb_strlen($command, '8bit'))) {
            throw new SocketException("Failed to write to socket. $written of $len bytes written.\nRedis command was: " . $command);
        }
        return $this->parseResponse(implode(' ', $params), $type);
    }

    private function parseResponse(string $command, string $socket): array|bool|null|string
    {
        if ($this->dyTimeout !== null) {
            stream_set_timeout(
                $this->$socket,
                $timeout = (int)$this->dyTimeout,
                (int)(($this->dyTimeout - $timeout) * 1000000)
            );
        }
        try {
            if (($line = @fgets($this->$socket)) === false) {
                throw new SocketException("Failed to read from socket.\nRedis command was: " . $command);
            }
        } finally {
            if ($this->dyTimeout !== null) {
                $this->dyTimeout = null;
                stream_set_timeout(
                    $this->$socket,
                    $timeout = (int)$this->dataTimeout,
                    (int)(($this->dataTimeout - $timeout) * 1000000)
                );
            }
        }

        $type = $line[0];
        $line = mb_substr($line, 1, -2, '8bit');
        switch ($type) {
            case '+': // Status reply
                if ($line === 'OK' || $line === 'PONG') {
                    return true;
                } else {
                    return $line;
                }
                // no break
            case '-': // Error reply
                throw new Exception("Redis error: " . $line . "\nRedis command was: " . $command);
            case ':': // Integer reply
                // no cast to int as it is in the range of a signed 64 bit integer
                return $line;
            case '$': // Bulk replies
                if ($line === '-1') {
                    return null;
                }
                $length = (int)$line + 2;
                $data = '';
                while ($length > 0) {
                    if (($block = fread($this->$socket, $length)) === false) {
                        throw new SocketException("Failed to read from socket.\nRedis command was: " . $command);
                    }
                    $data .= $block;
                    $length -= mb_strlen($block, '8bit');
                }

                return mb_substr($data, 0, -2, '8bit');
            case '*': // Multi-bulk replies
                $count = (int)$line;
                $data = [];
                for ($i = 0; $i < $count; $i++) {
                    $data[] = $this->parseResponse($command, $socket);
                }

                return $data;
            default:
                throw new Exception('Received illegal data from redis: ' . $line . "\nRedis command was: " . $command);
        }
    }

    public function __call($name, $params)
    {
        return $this->executeCommand($name, $params);
    }

    public function createConnection(): void
    {
        $this->open();
    }

    public function reconnect(): void
    {
        $this->open();
    }
}
