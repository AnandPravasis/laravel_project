<?php

use Illuminate\Support\Facades\Route;

Route::get('/', function () {
    return view('welcome');
});
// Debug route
Route::get('/debug-test', function () {
    return response()->json(['message' => 'Web route working!']);
});
Route::get('/phpinfo', function () {
    phpinfo();
});

