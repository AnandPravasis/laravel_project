<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\DataExtractionTypesController;

Route::get('data-extraction-types', [DataExtractionTypesController::class, 'index']);
Route::get('data-extraction-types/{id}', [DataExtractionTypesController::class, 'show']);
Route::post('get-software', [DataExtractionTypesController::class, 'getSoftware']);
Route::post('store-extraction', [DataExtractionTypesController::class, 'storeExtraction']);

