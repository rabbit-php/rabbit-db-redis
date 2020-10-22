<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis;

use Throwable;
use Rabbit\Base\App;
use Rabbit\Pool\PoolManager;
use Swoole\Coroutine\System;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\Inflector;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Pool\AbstractConnection;
use Rabbit\Base\Exception\NotSupportedException;

class Connection extends AbstractConnection
{
    use ClusterTrait;

    /**
     * @var string the hostname or ip address to use for connecting to the redis server. Defaults to 'localhost'.
     * If [[unixSocket]] is specified, hostname and [[port]] will be ignored.
     */
    private string $hostname = 'localhost';
    /**
     * @var integer the port to use for connecting to the redis server. Default port is 6379.
     * If [[unixSocket]] is specified, [[hostname]] and port will be ignored.
     */
    private int $port = 6379;
    /**
     * @var string the unix socket path (e.g. `/var/run/redis/redis.sock`) to use for connecting to the redis server.
     * This can be used instead of [[hostname]] and [[port]] to connect to the server using a unix socket.
     * If a unix socket path is specified, [[hostname]] and [[port]] will be ignored.
     */
    private ?string $unixSocket = null;
    /**
     * @var integer the redis database to use. This is an integer value starting from 0. Defaults to 0.
     * Since version 2.0.6 you can disable the SELECT command sent after connection by setting this property to `null`.
     */
    private int $database = 0;
    /**
     * @var float timeout to use for connection to redis. If not set the timeout set in php.ini will be used: `ini_get("default_socket_timeout")`.
     */
    private ?float $connectionTimeout = null;
    /**
     * @var float timeout to use for redis socket when reading and writing data. If not set the php default value will be used.
     */
    private ?float $dataTimeout = null;
    /**
     * @var integer Bitmask field which may be set to any combination of connection flags passed to [stream_socket_client()](http://php.net/manual/en/function.stream-socket-client.php).
     * Currently the select of connection flags is limited to `STREAM_CLIENT_CONNECT` (default), `STREAM_CLIENT_ASYNC_CONNECT` and `STREAM_CLIENT_PERSISTENT`.
     *
     * > Warning: `STREAM_CLIENT_PERSISTENT` will make PHP reuse connections to the same server. If you are using multiple
     * > connection objects to refer to different redis [[$database|databases]] on the same [[port]], redis commands may
     * > get executed on the wrong database. `STREAM_CLIENT_PERSISTENT` is only safe to use if you use only one database.
     * >
     * > You may still use persistent connections in this case when disambiguating ports as described
     * > in [a comment on the PHP manual](http://php.net/manual/en/function.stream-socket-client.php#105393)
     * > e.g. on the connection used for session storage, specify the port as:
     * >
     * > ```php
     * > 'port' => '6379/session'
     * > ```
     *
     * @see http://php.net/manual/en/function.stream-socket-client.php
     * @since 2.0.5
     */
    private int $socketClientFlags = STREAM_CLIENT_CONNECT;

    /**
     * @var resource redis socket connection
     */
    private $_socket = null;
    /**
     * @var resource redis socket connection
     */
    private $_socketSlave = null;
    /** @var bool */
    private bool $separate = false;

    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * @param bool $quit
     * @throws Exception
     * @throws Throwable
     */
    public function close(bool $quit = true)
    {
        if ($this->_socket !== null) {
            $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port) . ', database=' . $this->database;
            App::warning('Closing DB connection: ' . $connection, 'redis');
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

    /**
     * Executes a redis command.
     * For a list of available commands and their parameters see http://redis.io/commands.
     *
     * The params array should contain the params separated by white space, e.g. to execute
     * `SET mykey somevalue NX` call the following:
     *
     * ```php
     * $redis->executeCommand('SET', ['mykey', 'somevalue', 'NX']);
     * ```
     *
     * @param string $name the name of the command
     * @param array $params list of parameters for the command
     * @return array|bool|null|string Dependent on the executed command this method
     * will return different data types:
     *
     * - `true` for commands that return "status reply" with the message `'OK'` or `'PONG'`.
     * - `string` for commands that return "status reply" that does not have the message `OK` (since version 2.0.1).
     * - `string` for commands that return "integer reply"
     *   as the value is in the range of a signed 64 bit integer.
     * - `string` or `null` for commands that return "bulk reply".
     * - `array` for commands that return "Multi-bulk replies".
     *
     * See [redis protocol description](http://redis.io/topics/protocol)
     * for details on the mentioned reply types.
     * @throws Throwable for commands that return [error reply](http://redis.io/topics/protocol#error-reply).
     */
    public function executeCommand(string $name, array $params = [])
    {
        $name = strtoupper($name);
        $tmp = [];
        if ($this->_socketSlave && in_array($name, Redis::READ_COMMAND)) {
            $type = '_socketSlave';
        } else {
            $type = '_socket';
        }
        $this->parseParams($params, $tmp);
        $params = array_merge(explode(' ', $name), $tmp);
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen((string)$arg, '8bit') . "\r\n" . $arg . "\r\n";
        }
        App::debug("Executing Redis Command: {$name}", 'redis');
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
                App::error((string)$e, 'redis');
                $this->close(false);
                App::warning(sprintf('Redis connection retry host=%s port=%d,after %.3f', $this->hostname, $this->port, $this->retryDelay));
                System::sleep($this->retryDelay);
                $this->$type = null;
                $this->open();
            }
        }
    }

    /**
     * @param array $params
     * @param array $tmp
     */
    private function parseParams(array $params, array &$tmp): void
    {
        foreach ($params as $index => $item) {
            if (is_array($item)) {
                $this->parseParams($item, $tmp);
            } elseif (is_int($index)) {
                $tmp[] = $item;
            } else {
                $tmp = array_merge($tmp, [$index, $item]);
            }
        }
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws Throwable if connection fails
     */
    public function open()
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

    /**
     * @param array $config
     * @param string $type
     * @throws Throwable
     */
    private function makeConn(array $config, string $type): void
    {
        [$this->hostname, $this->port] = Redis::getCurrent($config, $type === '_socket' ? 'master' : 'slave');

        $this->database = isset($config['db']) && (0 <= $config['db'] && $config['db'] <= 16) ? intval($config['db']) : 0;
        $password = isset($config['password']) ? $config['password'] : null;
        $connection = ($this->unixSocket ?: $this->hostname . ':' . $this->port) . ', database=' . $this->database;
        App::debug('Opening redis DB connection: ' . $connection, 'redis');
        $this->$type = @stream_socket_client(
            $this->unixSocket ? 'unix://' . $this->unixSocket : 'tcp://' . $this->hostname . ':' . $this->port,
            $errorNumber,
            $errorDescription,
            $this->connectionTimeout ? $this->connectionTimeout : ini_get('default_socket_timeout'),
            $this->socketClientFlags
        );
        if ($this->$type) {
            if ($this->dataTimeout !== null) {
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
            App::error("Failed to open redis DB connection ($connection): $errorNumber - $errorDescription", 'redis');
            $message = getDI('debug') ? "Failed to open redis DB connection ($connection): $errorNumber - $errorDescription" : 'Failed to open DB connection.';
            throw new Exception($message . ';' . $errorDescription, $errorNumber);
        }
    }

    /**
     * @param string $uri
     * @return array
     * @throws Exception
     */
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
        $configs = array_merge($parseAry, $options);
        unset($configs['path']);
        unset($configs['query']);

        return $configs;
    }

    /**
     * @param string $command
     * @param array $params
     * @param string $type
     * @return array|bool|string|null
     * @throws Exception
     * @throws SocketException
     */
    private function sendCommandInternal(string $command, array $params, string $type)
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

    /**
     * @param string $command
     * @param string $socket
     * @return array|bool|string|null
     * @throws Exception
     * @throws SocketException
     */
    private function parseResponse(string $command, string $socket)
    {
        if (($line = @fgets($this->$socket)) === false) {
            throw new SocketException("Failed to read from socket.\nRedis command was: " . $command);
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
                if ($line == '-1') {
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

    /**
     * Allows issuing all supported commands via magic methods.
     *
     * ```php
     * $redis->hmset('test_collection', 'key1', 'val1', 'key2', 'val2')
     * ```
     *
     * @param string $name name of the missing method to execute
     * @param array $params method call arguments
     * @return mixed
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function __call($name, $params)
    {
        $redisCommand = strtoupper(Inflector::camel2words($name, false));
        return $this->executeCommand($redisCommand, $params);
    }

    /**
     * @throws Throwable
     */
    public function createConnection(): void
    {
        $this->open();
    }

    /**
     * @throws Throwable
     */
    public function reconnect(): void
    {
        $this->open();
    }
}
