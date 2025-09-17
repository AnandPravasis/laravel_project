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
public function updateRecords(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $type = DB::table('data_extraction')
        ->join('data_extraction_types', 'data_extraction.data_extraction_type_id', '=', 'data_extraction_types.id')
        ->where('data_extraction.id', $extractionId)
        ->select('data_extraction_types.software')
        ->first();

    if (!$type) {
        return response()->json([
            'status' => false,
            'message' => 'Invalid data_extraction_id or software not found.'
        ]);
    }

    $software = strtolower($type->software);
    $prefix = $software . '_';

    $phoneCounter = 2000000000;
    $kycCounter = 1;

    DB::beginTransaction();

    try {
        $memberTable = $prefix . 'member';
        if (!Schema::hasTable($memberTable)) {
            throw new \Exception("Table $memberTable does not exist.");
        }

        $members = DB::table($memberTable)
            ->where('data_extraction_id', $extractionId)
            ->get();

        foreach ($members as $member) {
            $updateData = [
                'mis_kyc_number' => $kycCounter++,
            ];

            if (!isset($member->phone) || !preg_match('/^\d{10}$/', $member->phone)) {
                $updateData['phone'] = strval($phoneCounter++);
            }

            DB::table($memberTable)
                ->where('id', $member->id)
                ->update($updateData);
        }
        DB::table('data_extraction_file_status')
            ->where('file', 'members')
            ->where('data_extraction_id', $extractionId)
            ->update(['status' => 1]);
        $memberMap = DB::table($memberTable)
            ->where('data_extraction_id', $extractionId)
            ->whereNotNull('class')
            ->whereNotNull('member_no')
            ->pluck('mis_kyc_number', DB::raw("CONCAT(class, '|', member_no)"))
            ->toArray();
        $files = DB::table('data_extraction_file_status')
            ->where('data_extraction_id', $extractionId)
            ->where('file', '!=', 'members')
            ->pluck('file');

        foreach ($files as $file) {
            $table = $prefix . strtolower($file);
            if (!Schema::hasTable($table)) continue;

            $hasClass = Schema::hasColumn($table, 'class');
            $hasMemberNo = Schema::hasColumn($table, 'member_no');
            $hasPhoneNo = Schema::hasColumn($table, 'phone_no');
            $hasKyc = Schema::hasColumn($table, 'mis_kyc_number');

            if (!$hasKyc) continue;

            $rows = DB::table($table)
                ->where('data_extraction_id', $extractionId)
                ->get();

            foreach ($rows as $row) {
                $update = [];
                if ($hasClass && $hasMemberNo && $row->class && $row->member_no) {
                    $key = $row->class . '|' . $row->member_no;
                    $update['mis_kyc_number'] = $memberMap[$key] ?? $kycCounter++;
                } else {
                    $update['mis_kyc_number'] = $kycCounter++;
                }
                if ($hasPhoneNo) {
                    if (!isset($row->phone_no) || !preg_match('/^\d{10}$/', $row->phone_no)) {
                        $update['phone_no'] = strval($phoneCounter++);
                    }
                }

                DB::table($table)
                    ->where('id', $row->id)
                    ->update($update);
            }
            DB::table('data_extraction_file_status')
                ->where('file', $file)
                ->where('data_extraction_id', $extractionId)
                ->update(['status' => 1]);
        }

        DB::commit();

        return response()->json([
            'status' => true,
            'message' => 'Data updated successfully for data_extraction_id: ' . $extractionId,
        ]);
    } catch (\Exception $e) {
        DB::rollBack();
        return response()->json([
            'status' => false,
            'message' => 'Error: ' . $e->getMessage(),
        ], 500);
    }
}

public function updateCsv(Request $request)
{
    $request->validate([
        'csv_file'           => 'required|file|mimes:csv,txt',
        'file_name'          => 'required|string',
        'data_extraction_id' => 'required|integer',
    ]);

    // Get the software name from the extraction ID
    $type = DB::table('data_extraction')
        ->join('data_extraction_types', 'data_extraction.data_extraction_type_id', '=', 'data_extraction_types.id')
        ->where('data_extraction.id', $request->data_extraction_id)
        ->select('data_extraction_types.software')
        ->first();

    if (!$type) {
        return response()->json([
            'status' => false,
            'message' => 'Invalid data_extraction_id or software not found.',
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
    DB::table($tableName)
        ->where('data_extraction_id', $request->data_extraction_id)
        ->delete();
    return $this->uploadCsv($request);
}

public function deleteFileData(Request $request)
{
    $request->validate([
        'file_name'          => 'required|string',
        'data_extraction_id' => 'required|integer',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $fileName = strtolower($request->input('file_name'));
    $type = DB::table('data_extraction')
        ->join('data_extraction_types', 'data_extraction.data_extraction_type_id', '=', 'data_extraction_types.id')
        ->where('data_extraction.id', $extractionId)
        ->select('data_extraction_types.software')
        ->first();

    if (!$type) {
        return response()->json([
            'status' => false,
            'message' => 'Invalid data_extraction_id or software not found.',
        ], 400);
    }

    $software = strtolower($type->software);
    $tableName = $software . '_' . $fileName;

    if (!Schema::hasTable($tableName)) {
        return response()->json([
            'status' => false,
            'message' => "Table '$tableName' does not exist.",
        ], 400);
    }

    try {
        DB::table($tableName)
            ->where('data_extraction_id', $extractionId)
            ->delete();
        DB::table('data_extraction_file_status')
            ->where('data_extraction_id', $extractionId)
            ->where('file', $fileName)
            ->update(['status' => 0]);

        return response()->json([
            'status' => true,
            'message' => "Data from '$tableName' deleted and status reset.",
        ]);
    } catch (\Exception $e) {
        return response()->json([
            'status' => false,
            'message' => 'Delete failed: ' . $e->getMessage(),
        ], 500);
    }
}

}
