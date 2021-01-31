<?php

namespace Upfor\ForkMan;

/**
 * ForkMan - A lightest process manager
 */
class ForkMan
{

    /**
     * @var string Slave process identifier
     */
    public static $slaveLabel = '-slave';

    /**
     * @var string Slave process command
     */
    public $slaveCmd;

    /**
     * @var string Process name
     */
    public $name;

    /**
     * @var int Process number
     */
    public $procNum = 2;

    /**
     * @var string Slave command prefix
     */
    public $prefix = '';

    /**
     * @var array Pool of process
     */
    private $procPool = [];

    /**
     * @var bool Is a slave process
     */
    private $isSlave = false;

    /**
     * @var int The number of busy
     */
    private $idleCount = 0;

    /**
     * @var callable Master process handler
     */
    private $masterHandler;

    /**
     * @var callable Slave process handler
     */
    private $slaveHandler;

    /**
     * @param  int    $procNum
     * @param  string $name
     */
    public function __construct($procNum = 2, $name = '')
    {
        if (empty($name)) {
            $name = explode('\\', __CLASS__);
            $name = end($name);
        }

        $this->name    = $name;
        $this->procNum = $procNum;

        if (!empty($_SERVER['argv']) && false !== array_search(static::$slaveLabel, $_SERVER['argv'])) {
            $this->isSlave = true;
        }
    }

    /**
     * Execute only in master process
     *
     * @param  callable $masterHandler master process callback, which can be call_user_func() execute
     * @return $this
     */
    public function master(callable $masterHandler)
    {
        if (!$this->isSlave) {
            $this->masterHandler = $masterHandler;
            $this->createMaster($this->procNum);
        }

        return $this;
    }

    /**
     * Create master handler
     *
     * @param  int $limit
     */
    private function createMaster($limit)
    {
        !$this->slaveCmd && $this->slaveCmd = $this->currentCmd();

        @cli_set_process_title($this->name . ':' . 'master');

        if (is_callable($this->masterHandler)) {
            for ($i = 0; $i < $limit; $i++) {
                $this->procPool[] = $this->createProcess();
            }

            call_user_func($this->masterHandler, $this);
        }
    }

    /**
     * Get current command
     *
     * @return string
     */
    private function currentCmd()
    {
        $prefix = empty($this->prefix) ? (!empty($_SERVER['_']) ? realpath($_SERVER['_']) : '/usr/bin/env php') : $this->prefix;
        $mixed  = array_merge([$prefix, $_SERVER['PHP_SELF']], $_SERVER['argv']);
        $mixed  = array_filter($mixed, function ($item) {
            return strpos($item, './') !== 0;
        });

        return implode(' ', array_unique($mixed));
    }

    /**
     * Create slave process
     *
     * @return array
     */
    private function createProcess()
    {
        $desc = [
            ['pipe', 'r'], // std input
            ['pipe', 'w'], // std output
            ['pipe', 'w'], // std error
        ];
        $res  = proc_open($this->slaveCmd . ' ' . static::$slaveLabel, $desc, $pipes, getcwd());

        $status = proc_get_status($res);
        if (!isset($status['pid'])) {
            $this->log('process create failed');

            return $this->createProcess();
        }

        $pid     = $status['pid'];
        $process = [
            'res'      => $res,
            'pipes'    => $pipes,
            'idle'     => true, // process is idling
            'pid'      => $pid,
            'callback' => null, // call when the slave process finished
        ];

        // non-blocking
        stream_set_blocking($pipes[1], 0);
        stream_set_blocking($pipes[2], 0);

        $this->log('start ' . $pid);

        return $process;
    }

    /**
     * Logger
     *
     * @param  mixed $info
     */
    public function log($info)
    {
        if (is_object($info)) {
            $info = var_export($info, true);
        } elseif (!is_scalar($info)) {
            $info = json_encode($info, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }
        $args = func_get_args();
        $line = count($args) > 1 ? call_user_func_array('sprintf', $args) : $info;

        $line = date('Y-m-d H:i:s') . ' ' . str_pad('[' . ($this->isSlave ? 'slave' : 'master') . ':' . getmypid() . '] ', 16, ' ', STR_PAD_RIGHT) . $line . "\n";

        error_log($line, 3, $this->isSlave ? 'php://stderr' : 'php://stdout');
    }

    /**
     * Execute only in slave process
     *
     * @param  callable $slaveHandler slave process callback, which can be call_user_func() execute
     * @return $this
     */
    public function slave(callable $slaveHandler)
    {
        if ($this->isSlave) {
            $this->slaveHandler = $slaveHandler;
            $this->createSlave();
        }

        return $this;
    }

    /**
     * Create slave handler
     */
    private function createSlave()
    {
        @cli_set_process_title($this->name . ':' . 'slave');

        while (true) {
            // listen input from master
            $fp   = @fopen('php://stdin', 'r');
            $recv = @fread($fp, 8); // read content length
            $size = intval(rtrim($recv));
            $data = @fread($fp, $size);
            @fclose($fp);

            if (!empty($data)) {
                if (is_callable($this->slaveHandler)) {
                    $data = json_decode($data, true);
                    $resp = call_user_func($this->slaveHandler, $data, $this);
                    echo json_encode($resp);
                }
            } else {
                usleep(100000);
            }
        }
    }

    /**
     * Master submit task to slave
     *
     * @param  mixed    $data
     * @param  callable $callback
     * @return int
     */
    public function submit($data, $callback = null)
    {
        if (!$this->isSlave) {
            $process             = &$this->getAvailableProcess();
            $process['callback'] = $callback;
            $data                = json_encode($data);
            $length              = strlen($data);
            $length              = str_pad($length . '', 8, ' ', STR_PAD_RIGHT);

            // send to slave process, with length and content
            fwrite($process['pipes'][0], $length . $data);

            return $process['pid'];
        }

        return null;
    }

    /**
     * Get available process
     *
     * @return mixed
     */
    private function &getAvailableProcess()
    {
        while (true) {
            $index = $this->check();
            if (isset($this->procPool[$index])) {
                $this->procPool[$index]['idle'] = false;
                $this->idleCount++;

                return $this->procPool[$index];
            }
            // sleep 50 ms
            usleep(50000);
        }
    }

    /**
     * Check process
     *
     * @return int
     */
    private function check()
    {
        $index = -1;
        foreach ($this->procPool as $key => &$process) {
            $this->checkProcessAlive($process);
            if (!$process['idle']) {
                echo stream_get_contents($process['pipes'][2]);      // std error
                $result = stream_get_contents($process['pipes'][1]); // std output
                if (!empty($result)) {
                    $process['idle'] = true;
                    $this->idleCount--;

                    if (is_callable($process['callback'])) {
                        $data = json_decode($result, true);
                        if (json_last_error()) {
                            $data = $result;
                        }
                        $process['callback']($data, $this);
                    }
                }
            }
            if ($process['idle'] && $index < 0) {
                $index = $key;
            }
        }

        return $index;
    }

    /**
     * Check a process is alive
     *
     * @param  array $process
     */
    private function checkProcessAlive(&$process)
    {
        $status = proc_get_status($process['res']);
        if (!$status['running']) {
            echo stream_get_contents($process['pipes'][2]);

            $this->killProcess($process);
            $this->log('close ' . $process['pid']);
            if (!$process['idle']) {
                $this->idleCount--;
            }
            $process = $this->createProcess();
        }
    }

    /**
     * Kill process
     *
     * @param  array $process
     * @return bool
     */
    private function killProcess($process)
    {
        if (function_exists('proc_terminate')) {
            return @proc_terminate($process['res']);
        } elseif (function_exists('posix_kill')) {
            return @posix_kill($process['pid'], 9);
        }

        return false;
    }

    /**
     * Loop condition
     *
     * @param  int $sleep (unit: ms)
     * @return bool
     */
    public function loop($sleep = 0)
    {
        if (!$this->isSlave) {
            if ($sleep > 0) {
                usleep($sleep * 1000);
            }

            $this->check();

            return true;
        }

        return false;
    }

    /**
     * Wait all process idled or timeout
     *
     * @param  int $timeout (unit: ms)
     */
    public function wait($timeout = 0)
    {
        $start = microtime(true);

        while (true) {
            $this->check();
            $interval = (microtime(true) - $start) * 1000;

            // timeout or all processes idle
            $outed = $timeout > 0 && $interval >= $timeout;
            if ($outed || $this->idleCount <= 0) {
                $killStatus = $this->killAllProcess();
                if ($killStatus) {
                    $this->log('all slave processes exited(' . ($outed ? 'timeout' : 'idle') . ')');

                    return;
                }
            }

            usleep(10000);
        }
    }

    /**
     * Kill all
     *
     * @return bool
     */
    private function killAllProcess()
    {
        $killStatus = true;
        foreach ($this->procPool as $process) {
            $status = $this->killProcess($process);
            if ($status) {
                $this->log('kill success: ' . $process['pid']);
                !$process['idle'] && $this->idleCount--;
            } else {
                $this->log('kill failed: ' . $process['pid']);
                $killStatus = false;
            }
        }

        return $killStatus;
    }

}
