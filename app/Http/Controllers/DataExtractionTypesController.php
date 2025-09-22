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
public function downloadSiliconKYC(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = 'silicon';
    $prefix = $software . '_';

    $header = [
        'Mobile', 'First Name', 'SurName', 'Parent/Guardian', 'Guardian Type',
        'Gender', 'Marital Status', 'DOB', 'Unique Identification Number (UID)/Aadhar',
        'House/Building Number', 'House Name', 'Area / Location', 'Village Town',
        'PIN', 'Tel. (Res.) / Alternate Mobile', 'Tel. (off) (XXXX-XXXXXX)',
        'Fax', 'Email', 'Net Worth', 'KYC Number', 'KYC SCCID', 'Status'
    ];

    $kycMap = [];

    $memberTable = $prefix . 'member';
    if (!Schema::hasTable($memberTable)) {
        return response()->json([
            'status' => false,
            'message' => "Member table not found.",
        ]);
    }

    $memberRows = DB::table($memberTable)
        ->where('data_extraction_id', $extractionId)
        ->whereNotNull('mis_kyc_number')
        ->get();

    foreach ($memberRows as $row) {
        $row = (array) $row;

        $kycMap[$row['mis_kyc_number']] = [
            'Mobile'       => $row['phone'] ?? '',
            'First Name'   => $row['name'] ?? '',
            'SurName'      => '',
            'Parent/Guardian' => $row['father_name'] ?? '',
            'Guardian Type'   => 'C/o',
            'Gender'       => $row['sex'] ?? '',
            'Marital Status' => '',
            'DOB'          => $row['dob'] ?? '',
            'UID'          => '',
            'House/Building Number' => $row['house_no'] ?? '',
            'House Name'   => $row['present_add1'] ?? '',
            'Area / Location' => $row['present_add2'] ?? '',
            'Village Town' => $row['present_add3'] ?? '',
            'PIN'          => '',
            'Tel. Res'     => '',
            'Tel. Off'     => '',
            'Fax'          => '',
            'Email'        => $row['e_mail_id'] ?? '',
            'Net Worth'    => '',
            'KYC Number'   => $row['mis_kyc_number'],
            'KYC SCCID'    => '',
            'Status'       => ''
        ];
    }

    // Load other tables with mis_kyc_number (excluding member table)
    $tables = DB::select("SHOW TABLES LIKE '{$prefix}%'");

$otherTables = collect($tables)
    ->map(function ($table) {
        return array_values((array)$table)[0]; // Get table name string
    })
    ->filter(function ($table) use ($memberTable) {
        return $table !== $memberTable && Schema::hasColumn($table, 'mis_kyc_number');
    })
    ->values()
    ->toArray();


    $fieldMapping = [
        'Mobile'               => ['phone', 'phone_no'],
        'First Name'           => ['name'],
        'Parent/Guardian'      => ['father_name'],
        'Gender'               => ['sex'],
        'DOB'                  => ['dob'],
        'House/Building Number'=> ['house_no'],
        'House Name'           => ['add1'],
        'Area / Location'      => ['add2'],
        'Village Town'         => ['add3'],
        'Email'                => ['e_mail_id'],
    ];

    foreach ($otherTables as $table) {
        $rows = DB::table($table)
            ->where('data_extraction_id', $extractionId)
            ->whereNotNull('mis_kyc_number')
            ->get();

        foreach ($rows as $row) {
            $row = (array) $row;
            $kycNum = $row['mis_kyc_number'];

            if (!isset($kycMap[$kycNum])) {
                continue; // skip KYC numbers not in member table
            }

            foreach ($fieldMapping as $csvKey => $possibleDbFields) {
                if (!empty($kycMap[$kycNum][$csvKey])) {
                    continue; // already set from member
                }
                foreach ($possibleDbFields as $field) {
                    if (!empty($row[$field])) {
                        $kycMap[$kycNum][$csvKey] = $row[$field];
                        break;
                    }
                }
            }
        }
    }

    // Convert to CSV
    $csvRows = [];
    $csvRows[] = $header;

    foreach ($kycMap as $record) {
        $csvRows[] = [
            $record['Mobile'],
            $record['First Name'],
            $record['SurName'],
            $record['Parent/Guardian'],
            $record['Guardian Type'],
            $record['Gender'],
            $record['Marital Status'],
            $record['DOB'],
            $record['UID'],
            $record['House/Building Number'],
            $record['House Name'],
            $record['Area / Location'],
            $record['Village Town'],
            $record['PIN'],
            $record['Tel. Res'],
            $record['Tel. Off'],
            $record['Fax'],
            $record['Email'],
            $record['Net Worth'],
            $record['KYC Number'],
            $record['KYC SCCID'],
            $record['Status'],
        ];
    }

    $filename = 'silicon_kyc_data_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->download($filepath);
}
public function downloadKYC(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software' => 'required|string',
    ]);

    $software = strtolower($request->input('software'));

    if ($software === 'silicon') {
        return $this->downloadSiliconKYC($request); // Call existing silicon logic
    }

    // else fallback to another general logic (or return unsupported for now)
    return response()->json([
        'status' => false,
        'message' => "KYC download not implemented for '$software' yet.",
    ]);
}


}
