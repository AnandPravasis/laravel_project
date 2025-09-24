<?php

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;
use App\Http\Controllers\DataExtractionTypesController;

Route::get('data-extraction-types', [DataExtractionTypesController::class, 'index']);
Route::get('data-extraction-types/{id}', [DataExtractionTypesController::class, 'show']);
Route::post('get-software', [DataExtractionTypesController::class, 'getSoftware']);
Route::post('add-bank', [DataExtractionTypesController::class, 'addBank']);
Route::post('get-bank/{id}', [DataExtractionTypesController::class, 'getBankById']);
Route::post('upload-file', [DataExtractionTypesController::class, 'uploadCsv']);
Route::post('extract-kyc', [DataExtractionTypesController::class, 'updateRecords']);
Route::post('update-csv', [DataExtractionTypesController::class, 'updateCsv']);
Route::post('delete-file', [DataExtractionTypesController::class, 'deleteFileData']);
Route::post('/download/kyc', [DataExtractionTypesController::class, 'downloadKYC']);
Route::post('/download/share-outstanding', [DataExtractionTypesController::class, 'downloadShareOutstandingCsv']);
Route::post('loan-outstanding', [DataExtractionTypesController::class, 'downloadLoanOutstandingCsv']);
Route::post('savings-outstanding', [DataExtractionTypesController::class, 'downloadSavingsOutstandingCsv']);
Route::post('daily-outstanding', [DataExtractionTypesController::class, 'downloadDepositOutstandingCsv']);
Route::post('fd-outstanding', [DataExtractionTypesController::class, 'downloadFdOutstandingCsv']);



