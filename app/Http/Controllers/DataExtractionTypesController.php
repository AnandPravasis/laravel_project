<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use App\Models\DataExtractionTypes;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;
use App\Models\DataExtraction;
use App\Models\DataExtractionFileStatus;

class DataExtractionTypesController extends Controller
{
    public function index()
    {
        $data = DataExtractionTypes::with('files','extractions.fileStatus')->get();
        
        if(!empty($data)){
            $array=[
              'status'  => true,
              'message' => "Successfully fetched.",
              'data'    => $data,
            ];
            return response()->json($array);die;
        }
        else{
            $array=[
                'status'=>false,
                'message'=>'Type not found',
                'data'=>[],
            ];
            return response()->json($array);die;
        }
    }

    public function getSoftware(Request $request)
    { 
        $validatedData = $request->validate([
            'software' => 'required'
        ]);
        $software = $validatedData['software'];
        $details = DataExtractionTypes::where('software', $software)
                    ->first();
        if(!empty($details)){
            $array=[
              'status'  => true,
              'message' => "Software data retrieved successfully.",
              'data'    => $details,
            ];
            return response()->json($array);die;
        }
        else{
            $array=[
                'status'=>false,
                'message'=>'Software not found',
                'data'=>[],
            ];
            return response()->json($array);die;
        }
    }
    public function AddBank(Request $request)
    {
        $validatedData = $request->validate([
            'bank_name' => 'required|string|max:255',
            'software'  => 'required|string|max:255',
        ]);
        $type = DataExtractionTypes::with('files')->where('software', $validatedData['software'])->first();

        if(empty($type)){
            $array = [
                'status'  => false,
                'message' => 'Invalid software type',
                'data'    => [],
            ];
            return response()->json($array);die;
        }
        DB::transaction(function () use ($validatedData, $type) {
            Log::info('Transaction started');
            $extraction = DataExtraction::create([
                'bank_name'              => $validatedData['bank_name'],
                'data_extraction_type_id'=> $type->id,
            ]);
            foreach($type->files as $file){
                DataExtractionFileStatus::create([
                    'data_extraction_id' => $extraction->id,
                    'file'               => $file->file,
                    'module'             => $file->module,
                ]);
            }
        });
        $array = [
            'status'  => true,
            'message' => "Data successfully inserted.",
            'data'    => [],
        ];
        return response()->json($array);die;
    }
    public function getAllBanks()
    {
        $banks = DataExtraction::with(['fileStatus', 'extractionType'])->get();

        if ($banks->isNotEmpty()) {
            return response()->json([
                'status' => true,
                'message' => 'All banks fetched successfully.',
                'data' => $banks
            ]);
        } else {
            return response()->json([
                'status' => false,
                'message' => 'No banks found.',
                'data' => []
            ]);
        }
    }
    public function getBankById($id)
    {
        $bank = DataExtraction::with(['fileStatus', 'extractionType'])->find($id);

        if ($bank) {
            return response()->json([
                'status' => true,
                'message' => 'Bank details retrieved successfully.',
                'data' => $bank
            ]);
        } else {
            return response()->json([
                'status' => false,
                'message' => 'Bank not found.',
                'data' => []
            ]);
        }
    }
    public function uploadCsv(Request $request)
{
    $request->validate([
        'csv_file'           => 'required|file|mimes:csv,txt',
        'file_name'          => 'required|string',
        'data_extraction_id' => 'required|integer',
    ]);
    $extraction = DataExtraction::find($request->data_extraction_id);
    if (!$extraction) {
        return response()->json([
            'status' => false,
            'message' => 'Invalid data_extraction_id',
        ], 400);
    }
    $type = DataExtractionTypes::find($extraction->data_extraction_type_id);

    if (!$type) {
        return response()->json([
            'status' => false,
            'message' => 'Invalid data_extraction_type_id',
        ], 400);
    }

    $software = strtolower($type->software);
    $tableName = $software . '_' . strtolower($request->file_name);

    if (!Schema::hasTable($tableName)) {
        return response()->json([
            'status' => false,
            'message' => "Table '$tableName' does not exist.",
        ], 400);
    }

    $csvFile = $request->file('csv_file');
    $handle = fopen($csvFile->getRealPath(), 'r');
    $header = null;
    $rows = [];
    $rowCount = 0;

    $tableColumns = null;
    while (($data = fgetcsv($handle, 1000, ',')) !== false) {
    if (!$header) {
        $header = array_map('trim', $data);
    } else {
        $row = array_combine($header, $data);
        if ($row) {
            if (is_null($tableColumns)) {
                $tableColumns = Schema::getColumnListing($tableName);
                $tableColumns[] = 'data_extraction_id';
            }
            $row['data_extraction_id'] = $request->data_extraction_id;
            $filteredRow = array_intersect_key($row, array_flip($tableColumns));
            foreach ($filteredRow as $key => $value) {
                if (is_string($value) && (strtoupper($value) === 'NULL' || trim($value) === '')) {
                    $filteredRow[$key] = null;
                }
            }

            $rows[] = $filteredRow;
            $rowCount++;
        }
    }
}
    fclose($handle);

    if ($rowCount === 0) {
        return response()->json([
            'status' => false,
            'message' => 'CSV is empty or has no valid rows.',
        ], 400);
    }

    try {
        foreach (array_chunk($rows, 500) as $chunk) {
            DB::table($tableName)->insert($chunk);
        }

        return response()->json([
            'status' => true,
            'message' => "Inserted $rowCount records into '$tableName' successfully.",
        ]);
    } catch (\Exception $e) {
        return response()->json([
            'status' => false,
            'message' => 'Insert failed: ' . $e->getMessage(),
        ], 500);
    }
}

}
