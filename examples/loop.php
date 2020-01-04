<?php


require dirname(__DIR__) . '/src/ForkMan.php';

use Upfor\ForkMan\ForkMan;

$fm = new ForkMan(2);
$fm->master(function (ForkMan $fm) {
    while ($fm->loop(1000)) {
        $fm->submit('https://api.myjson.com/bins/ladzj', function ($data) {
            echo $data, "\n";
        });
    }
})->slave(function ($url, ForkMan $fm) {
    $fm->log($url);
    return file_get_contents($url);
});
