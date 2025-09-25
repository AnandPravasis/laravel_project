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
    ini_set('max_execution_time', 600);
        ini_set('memory_limit', '512M');
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
public function downloadShareOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
        'class'              => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code'); 
    $schemeCode = $request->input('scheme_code'); 
    $class = $request->input('class');  

    $prefix = $software . '_';
    $memberTable = $prefix . 'member';
    $shareTable = $prefix . 'share';
    $repayTable = $prefix . 'share_repay';

    if (!Schema::hasTable($memberTable) || !Schema::hasTable($shareTable) || !Schema::hasTable($repayTable)) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    $members = DB::table($memberTable)
        ->where('data_extraction_id', $extractionId)
        ->where('class', $class)
        ->get();

    $kycMap = $members->pluck('mis_kyc_number', 'member_no')->toArray();
    $nameMap = $members->pluck('name', 'member_no')->toArray();
    $dateMap = $members->pluck('adm_date', 'member_no')->toArray();

    // Load shares
    $shares = DB::table($shareTable)
        ->where('data_extraction_id', $extractionId)
        ->where('class', $class)
        ->select('member_no', DB::raw('SUM(amount) as total_amount'), DB::raw('SUM(no_of_share) as total_shares'))
        ->groupBy('member_no')
        ->get()
        ->keyBy('member_no');

    // Load share repayments
    $repayments = DB::table($repayTable)
        ->where('data_extraction_id', $extractionId)
        ->where('class', $class)
        ->select('member_no', DB::raw('SUM(amount) as total_amount'), DB::raw('SUM(no_of_share) as total_shares'))
        ->groupBy('member_no')
        ->get()
        ->keyBy('member_no');

    $csvHeader = [
        'Cost Center', 'Branch', 'Branch Code', 'Scheme Code', 'KYC. NO.',
        'Bill No /M#', 'Op Date', 'AMOUNT', 'Maturity Date', 'Blank',
        'Allow Zero Bal', 'SHARE HOLDER NAME', 'NO.OF SHARES', 'Applicant Type',
        'Reservation Type', 'Ward for election'
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($members as $member) {
        $memberNo = $member->member_no;

        // Compute bill number
        $memberNoFormatted = $memberNo >= 0
            ? str_pad($memberNo, 6, '0', STR_PAD_LEFT)
            : str_pad($memberNo, 6, '9', STR_PAD_LEFT);

        $billNo = $branchCode . $schemeCode . $memberNoFormatted;

        // Get amounts
        $credit = isset($shares[$memberNo]) ? $shares[$memberNo]->total_amount : 0;
        $debit = isset($repayments[$memberNo]) ? $repayments[$memberNo]->total_amount : 0;
        $amount = $credit - $debit;

        // Get shares
        $shareQtyCredit = isset($shares[$memberNo]) ? $shares[$memberNo]->total_shares : 0;
        $shareQtyDebit = isset($repayments[$memberNo]) ? $repayments[$memberNo]->total_shares : 0;
        $netShares = $shareQtyCredit - $shareQtyDebit;

        if ($amount == 0 && $netShares == 0) {
            continue; // Skip accounts with 0 shares and 0 balance
        }

        $csvRows[] = [
            '', // Cost Center
            '', // Branch
            $branchCode,
            $schemeCode,
            $member->mis_kyc_number,
            $billNo,
            $member->adm_date,
            number_format($amount, 2, '.', ''),
            '', '', '', // Maturity Date, Blank, Allow Zero Bal
            $member->name,
            $netShares,
            '', '', '', // Applicant Type, Reservation Type, Ward for election
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No share outstanding data found for export.',
        ]);
    }

    $filename = 'share_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->download($filepath);
}
public function downloadDepositOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
        'agent_code'         => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');
    $agentCode = $request->input('agent_code');

    $prefix = $software . '_';
    $openingTable = $prefix . 'dd_opening';
    $transactionTable = $prefix . 'dd_trn';

    // Check if required tables exist
    if (!Schema::hasTable($openingTable) || !Schema::hasTable($transactionTable)) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    // Load opening data
    $openings = DB::table($openingTable)
        ->where('data_extraction_id', $extractionId)
        ->where('agent_code', $agentCode)
        ->where('closed', 'N') // Only open accounts
        ->get();
    // Customer data mappings: KYC, Name, Maturity Date, Interest Rate, etc.
    $kycMap = $openings->pluck('mis_kyc_number', 'ac_no')->toArray();
    $nameMap = $openings->pluck('name', 'ac_no')->toArray();
    $maturityDateMap = $openings->pluck('maturity_date', 'ac_no')->toArray();
    $interestRateMap = $openings->pluck('int_rate', 'ac_no')->toArray();
    $trnDateMap = $openings->pluck('trn_date', 'ac_no')->toArray(); // Assuming 'trn_date' is available in dd_opening table

    // Load transaction data to calculate balance (sum of amounts)
    $transactions = DB::table($transactionTable)
    ->where('data_extraction_id', $extractionId)
    ->where('agent_code', $agentCode)
    ->select('ac_no', DB::raw('SUM(amount) as total_amount'))
    ->groupBy('ac_no')
    ->get()
    ->keyBy('ac_no');

    // CSV headers
    $csvHeader = [
         'Cost Center', 'Branch', 'Scheme Code', 'KYC Number', 'Bill No', 'Op Date', 'Balance',
        'Maturity Date', 'Interest Rate', 'Allow Zero', 'Customer Name', 'Last Interest Paid Date'
    ];

    // Prepare CSV rows
    $csvRows = [];
    $csvRows[] = $csvHeader;


    // Iterate through opening accounts and generate CSV rows
    foreach ($openings as $opening) {
        $accountNo = $opening->ac_no;

        $accountNoFormatted = $accountNo >= 0
    ? str_pad($accountNo, 6, '0', STR_PAD_LEFT)
    : str_pad(abs($accountNo), 6, '9', STR_PAD_LEFT);

    $billNo = $branchCode . $schemeCode . $accountNoFormatted;

        // Get balance (sum of all transaction amounts for this account)
        $balance = isset($transactions[$accountNo]) ? $transactions[$accountNo]->total_amount : 0;

        // Check the KYC number, name, and maturity date
        $kycNumber = $kycMap[$accountNo] ?? '';
        $customerName = $nameMap[$accountNo] ?? '';
        $maturityDate = $maturityDateMap[$accountNo] ?? '';
        $interestRate = $interestRateMap[$accountNo] ?? '';

        // Allow Zero balance - leave it blank as per the requirement
        $allowZero = '';

        // Last Interest Paid Date - Placeholder or logic for calculating last interest paid
        $lastInterestPaidDate = ''; // Add logic here if needed

        // Opening date from 'trn_date' in dd_opening table
        $opDate = $trnDateMap[$accountNo] ?? '';

        // // Skip account with zero balance if needed
        // if ($balance == 0) {
        //     continue;
        // }

        // Prepare row for the CSV file
        $csvRows[] = [
            '', // Cost Center (Blank)
            '', // Branch (Blank)
            $schemeCode,
            $kycNumber,
            $billNo,
            $opDate, // Assuming 'trn_date' corresponds to opening date
            number_format($balance, 2, '.', ''),
            $maturityDate,
            $interestRate,
            $allowZero,
            $customerName,
            $lastInterestPaidDate
        ];
    }

    // If no data found, return an error message
    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No deposit outstanding data found for export.',
        ]);
    }

    // Generate file and prepare for download
    $filename = 'deposit_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    // Write CSV to file
    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->download($filepath);
}

public function downloadLoanOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
        'loan_code'          => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');
    $loanCode = $request->input('loan_code');

    $prefix = $software . '_';

    $memberTable = $prefix . 'member';
    $loanMembersTable = $prefix . 'loan_members';
    $loanPaymentTable = $prefix . 'loan_payment';
    $loanRepaymentTable = $prefix . 'loan_repayment';

    if (
        !Schema::hasTable($memberTable) ||
        !Schema::hasTable($loanMembersTable) ||
        !Schema::hasTable($loanPaymentTable) ||
        !Schema::hasTable($loanRepaymentTable)
    ) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    // Get all loan accounts for this loan_code and extraction
    $loanAccounts = DB::table($loanMembersTable)
        ->where('data_extraction_id', $extractionId)
        ->where('loan_code', $loanCode)
        ->get();
    if ($loanAccounts->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No loan accounts found.',
        ]);
    }

    // Get composite keys for members: (member_no, class)
    $memberKeys = $loanAccounts->map(function ($loan) {
        return $loan->member_no . '__' . $loan->class;
    })->toArray();

    // Load members using composite keys
    $members = DB::table($memberTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn(DB::raw("CONCAT(member_no, '__', class)"), $memberKeys)
        ->get()
        ->keyBy(function ($item) {
            return $item->member_no . '__' . $item->class;
        });

    // Get loan numbers
    $loanNos = $loanAccounts->pluck('loan_no')->toArray();

    // Loan payment details
    $loanPayments = DB::table($loanPaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->where('loan_code', $loanCode)
        ->where('closed', 'N')
        ->whereIn('loan_no', $loanNos)
        ->get()
        ->keyBy('loan_no');
    // Loan repayments (grouped)
    $repayments = DB::table($loanRepaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->where('loan_code', $loanCode)
        ->whereIn('loan_no', $loanNos)
        ->select('loan_no', DB::raw('SUM(principal) as total_principal'), DB::raw('MAX(interest_upto) as last_interest_paid'))
        ->groupBy('loan_no')
        ->get()
        ->keyBy('loan_no');

    $csvHeader = [
        'scheme code',
        'branch',
        'kyc no',
        'loan no',
        'opening date',
        'loan amount',
        'balance',
        'due date',
        'interest rate',
        'name',
        'last interest paid date',
        'total_installments',
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($loanAccounts as $loan) {
        $loanNo = $loan->loan_no;
        $memberNo = $loan->member_no;
        $class = $loan->class;

        $memberKey = $memberNo . '__' . $class;

        $member = $members[$memberKey] ?? null;
        $loanPayment = $loanPayments[$loanNo] ?? null;

        if (!$member || !$loanPayment) {
            continue;
        }

        $totalPrincipalPaid = $repayments[$loanNo]->total_principal ?? 0;
        $lastInterestDate = $repayments[$loanNo]->last_interest_paid ?? '';

        $loanAmount = $loanPayment->amount;
        $balance = $loanAmount - $totalPrincipalPaid;

        if ($balance <= 0) {
            continue; // Skip fully paid
        }

        $loanNoFormatted = $loanNo >= 0
            ? str_pad($loanNo, 6, '0', STR_PAD_LEFT)
            : str_pad($loanNo, 6, '9', STR_PAD_LEFT);

        $billNo = $branchCode . $schemeCode . $loanNoFormatted;

        $csvRows[] = [
            $schemeCode,
            '', // branch
            $member->mis_kyc_number,
            $billNo,
            $loanPayment->repay_start_date,
            number_format($loanAmount, 2, '.', ''),
            number_format($balance, 2, '.', ''),
            $loanPayment->due_date,
            $loanPayment->int_rate,
            $member->name,
            $lastInterestDate,
            '', // total_installments
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No loan outstanding data found for export.',
        ]);
    }

    $filename = 'loan_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->download($filepath);
}
public function downloadSavingsOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');

    $prefix = $software . '_';
    $sbMasterTable = $prefix . 'sb_master';
    $sbTrnTable = $prefix . 'sb_trn';
    $interestRateTable = $prefix . 'sb_interest_rate';

    if (!Schema::hasTable($sbMasterTable) || !Schema::hasTable($sbTrnTable) || !Schema::hasTable($interestRateTable)) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    $interestRate = DB::table($interestRateTable)
    ->where('data_extraction_id', $extractionId)
    ->value('interest_rate') ?? '';

    $accounts = DB::table($sbMasterTable)
        ->where('data_extraction_id', $extractionId)
        ->where('closed', 'N')
        ->get();
    if ($accounts->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No eligible savings accounts found.',
        ]);
    }

    $kycMap   = $accounts->pluck('mis_kyc_number', 'ac_no')->toArray();
    $nameMap  = $accounts->pluck('name', 'ac_no')->toArray();
    $dateMap  = $accounts->pluck('op_date', 'ac_no')->toArray();

    $transactions = DB::table($sbTrnTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('ac_no', array_keys($kycMap))
        ->select('ac_no', DB::raw('SUM(credit) as total_credit'), DB::raw('SUM(debit) as total_debit'))
        ->groupBy('ac_no')
        ->get();

    $balanceMap = [];
    foreach ($transactions as $trn) {
        $balanceMap[$trn->ac_no] = ($trn->total_credit ?? 0) - ($trn->total_debit ?? 0);
    }

    $header = [
        'CC', 'Branch', 'SchemeCode', 'KYC NUMBER', 'Bill No',
        'Op Date', 'BALANCE', 'MATURITY DATE', 'INTEREST RATE',
        'Allow Zero', 'CUSTOMER NAME', 'Last interest paid date'
    ];

    $csvRows = [];
    $csvRows[] = $header;

    $lastInterestDate = now()->month > 3
        ? now()->copy()->startOfYear()->subDay()->format('d-m-Y')   // 31-03 of current year
        : now()->copy()->subYear()->endOfMonth()->format('d-m-Y'); // 31-03 of last year
            // $lastInterestDateRaw = DB::table($interestRateTable)
            //     ->where('data_extraction_id', $extractionId)
            //     ->value('int_upto');

            // if ($lastInterestDateRaw) {
            //     $dateObj = new \DateTime($lastInterestDateRaw);
            //     $lastInterestDate = $dateObj->format('d-m-Y');
            // } else {
            //     $lastInterestDate = '';
            // }

    foreach ($kycMap as $ac_no => $kycNo) {
        $balance = $balanceMap[$ac_no] ?? 0;

        if ($balance == 0) {
            continue;
        }

        // $memberNoFormatted = str_pad($ac_no, 6, '0', STR_PAD_LEFT);
        $memberNoFormatted = $ac_no >= 0
        ? str_pad($ac_no, 6, '0', STR_PAD_LEFT)
        : str_pad(abs($ac_no), 6, '9', STR_PAD_LEFT);
        $billNo = $branchCode . $schemeCode . $memberNoFormatted;

        $csvRows[] = [
            '', // CC
            $branchCode,
            $schemeCode,
            $kycNo,
            $billNo,
            $dateMap[$ac_no] ?? '',
            number_format($balance, 2, '.', ''),
            '', // Maturity Date
            $interestRate,
            '', // Allow Zero
            $nameMap[$ac_no] ?? '',
            $lastInterestDate,
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No savings outstanding data found for export.',
        ]);
    }

    $filename = 'savings_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->download($filepath);
}

public function downloadFdOutstandingCsv(Request $request)
{
    // Validate the request parameters
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');

    $prefix = $software . '_';
    $fdHdTable = $prefix . 'fd_hd';
    $fdDtTable = $prefix . 'fd_dt';

    // Check if the required tables exist
    if (!Schema::hasTable($fdHdTable) || !Schema::hasTable($fdDtTable)) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    // Fetch the FD accounts
    $fds = DB::table($fdHdTable)
        ->where('data_extraction_id', $extractionId)
        ->where('closed', 'N') // Only open FDs
        ->get();
    
    if ($fds->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No eligible FD accounts found.',
        ]);
    }

    // Fetch the FD transactions (deposit records)
    $fdTransactions = DB::table($fdDtTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('fd_no', $fds->pluck('fd_no'))
        ->get();

    // Organize the data for each FD account
    $fdData = [];
    foreach ($fds as $fd) {
        // Get all related deposit transactions for this FD
        $transactions = $fdTransactions->where('fd_no', $fd->fd_no);

        // Find the earliest deposit date and the corresponding amount for the latest maturity date
        $earliestDeposit = $transactions->sortBy('dep_date')->first();
        $latestMaturity = $transactions->sortByDesc('mat_date')->first();

        // Get maturity date based on renew_flag
        $dueDate = $latestMaturity->mat_date;
        if ($latestMaturity->renew_flag === 'R') {
            $dueDate = $this->calculateRenewalDate($latestMaturity->mat_date, $latestMaturity->period, $latestMaturity->term);
        }
            $fdNoFormatted = $fd->fd_no >= 0
        ? str_pad($fd->fd_no, 6, '0', STR_PAD_LEFT)
        : str_pad(abs($fd->fd_no), 6, '9', STR_PAD_LEFT);

        $fdData[] = [
            'cost_center'      => '', // Blank column
            'branch'           => $branchCode,
            'scheme_code'      => $schemeCode,
            'kyc_no'           => $fd->mis_kyc_number,
            'fd_no'            => $branchCode . $schemeCode . $fdNoFormatted,
            'opening_date'     => $earliestDeposit->dep_date,
            'balance'          => $latestMaturity->amount,
            'due_date'         => $dueDate,
            'interest_rate'    => $latestMaturity->int_rate,
            'allow_zero'       => '', // Blank column
            'name'             => $fd->name,
            'last_interest_paid_date' => $latestMaturity->renew_flag === 'R' ? $latestMaturity->mat_date : $earliestDeposit->dep_date,
        ];
    }

    if (count($fdData) === 0) {
        return response()->json([
            'status' => false,
            'message' => 'No FD outstanding data found for export.',
        ]);
    }

    // Prepare CSV file
    $header = [
        'Cost Center', 'Branch', 'Scheme Code', 'KYC No', 'FD No', 
        'Opening Date', 'Balance', 'Due Date', 'Interest Rate', 
        'Allow Zero', 'Name', 'Last Interest Paid Date'
    ];

    $csvRows = [$header];

    foreach ($fdData as $fd) {
        $csvRows[] = [
            $fd['cost_center'],
            $fd['branch'],
            $fd['scheme_code'],
            $fd['kyc_no'],
            $fd['fd_no'],
            $fd['opening_date'],
            number_format($fd['balance'], 2, '.', ''),
            $fd['due_date'],
            $fd['interest_rate'],
            $fd['allow_zero'],
            $fd['name'],
            $fd['last_interest_paid_date'],
        ];
    }

    // Generate the file
    $filename = 'fd_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->json([
            'status' => true,
            'file_url' => asset("storage/" . $filename),
            'file_name' => $filename,
            'message' => 'FD outstanding exported successfully',
        ]);
}


private function calculateRenewalDate($maturityDate, $period, $term)
{
    $maturityDateObj = new \DateTime($maturityDate);
    
    switch (strtoupper($term)) {
        case 'Y': // Add years
            $maturityDateObj->modify("+$period year");
            break;
        case 'M': // Add months
            $maturityDateObj->modify("+$period month");
            break;
        case 'D': // Add days
            $maturityDateObj->modify("+$period day");
            break;
        default:
            return $maturityDateObj->format('Y-m-d');
    }
    
    return $maturityDateObj->format('Y-m-d');
}

public function downloadGroupDepositOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
        'group_code'         => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software     = strtolower($request->input('software'));
    $branchCode   = $request->input('branch_code');
    $schemeCode   = $request->input('scheme_code');
    $groupCode    = $request->input('group_code');
    $prefix = $software . '_';
    $acMasterTable = $prefix . 'group_deposit_ac_master';
    $trnTable      = $prefix . 'group_deposit_trn_dt';

    if (!Schema::hasTable($acMasterTable) || !Schema::hasTable($trnTable)) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    // Fetch account details
    $accounts = DB::table($acMasterTable)
        ->where('data_extraction_id', $extractionId)
        ->where('group_code', $groupCode)
        ->where('closed', 'N')
        ->get();

    if ($accounts->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No active group deposit accounts found.',
        ]);
    }

    // Maps for quick lookup
    $acNos         = $accounts->pluck('ac_no')->toArray();
    $kycMap        = $accounts->pluck('mis_kyc_number', 'ac_no')->toArray();
    $nameMap       = $accounts->pluck('name', 'ac_no')->toArray();
    $openingMap    = $accounts->pluck('opening_date', 'ac_no')->toArray();

    // Get transaction sums
    $transactions = DB::table($trnTable)
        ->where('data_extraction_id', $extractionId)
        ->where('group_code', $groupCode)
        ->whereIn('ac_no', $acNos)
        ->select('ac_no', DB::raw('SUM(amount) as total_amount'))
        ->groupBy('ac_no')
        ->get();
    $amountMap = [];
    foreach ($transactions as $trn) {
        $amountMap[$trn->ac_no] = $trn->total_amount ?? 0;
    }

    // CSV Header
    $header = [
        'scheme code', 'gds number', 'opening date', 'kyc no',
        'auction status', 'set off date', 'Auction date', 'Auction amount',
        'gds balance', 'interest balance', 'legal charge'
    ];

    $csvRows = [];
    $csvRows[] = $header;

    foreach ($acNos as $ac_no) {
        $balance = $amountMap[$ac_no] ?? 0;

        // if ($balance == 0) {
        //     continue;
        // }

        // Format gds number
        $memberNoFormatted = $ac_no >= 0
            ? str_pad($ac_no, 6, '0', STR_PAD_LEFT)
            : str_pad(abs($ac_no), 6, '9', STR_PAD_LEFT);
        $gdsNumber = $branchCode . $schemeCode . $memberNoFormatted;

        $csvRows[] = [
            $schemeCode,
            $gdsNumber,
            $openingMap[$ac_no] ?? '',
            $kycMap[$ac_no] ?? '',
            '', '', '', '', // auction status, set off, auction date, auction amount
            number_format($balance, 2, '.', ''),
            '', // interest balance
            '', // legal charge
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No group deposit outstanding data found for export.',
        ]);
    }

    $filename = 'group_deposit_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->json([
            'status' => true,
            'file_url' => asset("storage/" . $filename),
            'file_name' => $filename,
            'message' => 'GD outstanding exported successfully',
        ]);
}
public function downloadGoldOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');

    $prefix = $software . '_';

    $memberTable       = $prefix . 'member';
    $jlPaymentTable    = $prefix . 'jl_payment';
    $jlRepaymentTable  = $prefix . 'jl_repayment';
    $jewelDetailTable  = $prefix . 'jewel_detail';

    if (
        !Schema::hasTable($memberTable) ||
        !Schema::hasTable($jlPaymentTable) ||
        !Schema::hasTable($jlRepaymentTable) ||
        !Schema::hasTable($jewelDetailTable)
    ) {
        return response()->json(['status' => false, 'message' => 'Required tables are missing.'], 400);
    }

    $payments = DB::table($jlPaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->where('closed', 'N')
        ->get();

    if ($payments->isEmpty()) {
        return response()->json(['status' => false, 'message' => 'No gold loan data found.']);
    }

    $loanNos = $payments->pluck('loan_no')->toArray();
    $memberKeys = $payments->map(fn($row) => $row->member_no . '__' . $row->class)->unique()->toArray();

    // Get members
    $members = DB::table($memberTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn(DB::raw("CONCAT(member_no, '__', class)"), $memberKeys)
        ->get()
        ->keyBy(fn($item) => $item->member_no . '__' . $item->class);
    // Repayments grouped by loan_no
    $repayments = DB::table($jlRepaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->select('loan_no', DB::raw('SUM(principal) as total_principal'), DB::raw('MAX(int_upto) as last_interest_paid'))
        ->groupBy('loan_no')
        ->get()
        ->keyBy('loan_no');

    // Jewel details grouped by loan_no
    $jewelDetails = DB::table($jewelDetailTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->select('loan_no', DB::raw('SUM(nos) as total_nos'))
        ->groupBy('loan_no')
        ->get()
        ->keyBy('loan_no');

    $csvHeader = [
        'kyc number',
        'gold loan number',
        'scheme code',
        'Loan amount',
        'loan balance',
        'balance interest',
        'opened date',
        'loan maturity date',
        'int calc date',
        'ornament count',
        'total weight',
        'interest rate',
        'name',
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($payments as $payment) {
        $loanNo = $payment->loan_no;
        $memberKey = $payment->member_no . '__' . $payment->class;
        $member = $members[$memberKey] ?? null;
        if (!$member) continue;

        $repayment = $repayments[$loanNo] ?? null;
        $principalPaid = $repayment->total_principal ?? 0;
        $lastInterestDate = $repayment->last_interest_paid ?? '';

        $loanAmount = $payment->amount;
        $balance = $loanAmount - $principalPaid;

        if ($balance <= 0) continue;

        $formattedLoanNo = $loanNo >= 0
            ? str_pad($loanNo, 6, '0', STR_PAD_LEFT)
            : str_pad($loanNo, 6, '9', STR_PAD_LEFT);
        $billNo = $branchCode . $schemeCode . $formattedLoanNo;

        $jewelCount = $jewelDetails[$loanNo]->total_nos ?? 0;

        $csvRows[] = [
            $member->mis_kyc_number,
            $billNo,
            $schemeCode,
            number_format($loanAmount, 2, '.', ''),
            number_format($balance, 2, '.', ''),
            '', // balance interest blank
            $payment->loan_date,
            $payment->due_date,
            $lastInterestDate,
            $jewelCount,
            number_format($payment->net_wt, 2, '.', ''),
            $payment->int_rate,
            $member->name,
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json(['status' => false, 'message' => 'No gold loan data found for export.']);
    }

    $filename = 'gold_loan_summary_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->json([
            'status' => true,
            'file_url' => asset("storage/" . $filename),
            'file_name' => $filename,
            'message' => 'Gold loan outstanding exported successfully',
        ]);
}
public function downloadGoldLoanJewelDetailsCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software = strtolower($request->input('software'));
    $branchCode = $request->input('branch_code');
    $schemeCode = $request->input('scheme_code');

    $prefix = $software . '_';

    $jlPaymentTable   = $prefix . 'jl_payment';
    $jewelDetailTable = $prefix . 'jewel_detail';

    if (
        !Schema::hasTable($jlPaymentTable) ||
        !Schema::hasTable($jewelDetailTable)
    ) {
        return response()->json(['status' => false, 'message' => 'Required tables are missing.'], 400);
    }

    $payments = DB::table($jlPaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->where('closed', 'N')
        ->get();

    if ($payments->isEmpty()) {
        return response()->json(['status' => false, 'message' => 'No gold loan data found.']);
    }

    $loanNos = $payments->pluck('loan_no')->toArray();

    $jewels = DB::table($jewelDetailTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->get();

    $paymentLookup = $payments->keyBy('loan_no');

    $csvHeader = [
        'gold loan number',
        'ornament type',
        'ornament quantity',
        'ornament weight',
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($jewels as $jewel) {
        $loanNo = $jewel->loan_no;
        $payment = $paymentLookup[$loanNo] ?? null;
        if (!$payment) continue;

        $formattedLoanNo = $loanNo >= 0
            ? str_pad($loanNo, 6, '0', STR_PAD_LEFT)
            : str_pad($loanNo, 6, '9', STR_PAD_LEFT);

        $billNo = $branchCode . $schemeCode . $formattedLoanNo;

        $csvRows[] = [
            $billNo,
            $jewel->jewel_name,
            $jewel->nos,
            number_format($jewel->net_wt, 2, '.', ''),
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json(['status' => false, 'message' => 'No jewel detail data found for export.']);
    }

    $filename = 'gold_loan_jewel_details_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

 return response()->json([
            'status' => true,
            'file_url' => asset("storage/" . $filename),
            'file_name' => $filename,
            'message' => 'Gold loan outstanding exported successfully',
        ]);
}
public function downloadGdLoanOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software     = strtolower($request->input('software'));
    $branchCode   = $request->input('branch_code');
    $schemeCode   = $request->input('scheme_code');

    $prefix = $software . '_';

    $groupAcMasterTable = $prefix . 'group_deposit_ac_master';
    $gdLoanSecurityTable = $prefix . 'gd_loan_security';
    $gdLoanPaymentTable = $prefix . 'gd_loan_payment';
    $gdLoanRepayTable = $prefix . 'gd_loan_repay';

    if (
        !Schema::hasTable($groupAcMasterTable) ||
        !Schema::hasTable($gdLoanSecurityTable) ||
        !Schema::hasTable($gdLoanPaymentTable) ||
        !Schema::hasTable($gdLoanRepayTable)
    ) {
        return response()->json([
            'status' => false,
            'message' => 'Required tables are missing.',
        ], 400);
    }

    // Get all loan security entries
    $loanEntries = DB::table($gdLoanSecurityTable)
        ->where('data_extraction_id', $extractionId)
        ->get();

    if ($loanEntries->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No GD loan entries found.',
        ]);
    }

    // Generate composite keys (dep_group_code__dep_ac_no)
    $groupKeys = $loanEntries->map(function ($item) {
        return $item->dep_group_code . '__' . $item->dep_ac_no;
    })->toArray();

    // Get group account info
    $groupAccounts = DB::table($groupAcMasterTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn(DB::raw("CONCAT(group_code, '__', ac_no)"), $groupKeys)
        ->get()
        ->keyBy(function ($item) {
            return $item->group_code . '__' . $item->ac_no;
        });

    // Get loan numbers
    $loanNos = $loanEntries->pluck('loan_no')->toArray();

    // Get payment details
    $loanPayments = DB::table($gdLoanPaymentTable)
        ->where('data_extraction_id', $extractionId)
        ->where('closed', 'N')
        ->whereIn('loan_no', $loanNos)
        ->get()
        ->keyBy('loan_no');

    // Get repayments grouped
    $repayments = DB::table($gdLoanRepayTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->select('loan_no', DB::raw('SUM(principal) as total_principal'), DB::raw('MAX(int_upto) as last_interest_paid'))
        ->groupBy('loan_no')
        ->get()
        ->keyBy('loan_no');

    $csvHeader = [
        'scheme code',
        'branch',
        'kyc number',
        'loan number',
        'opened date',
        'loan amount',
        'loan balance',
        'due date',
        'interest rate',
        'name',
        'last interest paid date',
        'total installments',
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($loanEntries as $loan) {
        $groupKey = $loan->dep_group_code . '__' . $loan->dep_ac_no;
        $group = $groupAccounts[$groupKey] ?? null;
        $payment = $loanPayments[$loan->loan_no] ?? null;

        if (!$group || !$payment) {
            continue;
        }

        $repay = $repayments[$loan->loan_no] ?? null;

        $totalPrincipalPaid = $repay->total_principal ?? 0;
        $lastInterestPaid = $repay->last_interest_paid ?? '';

        $balance = $loan->loan_amount - $totalPrincipalPaid;

        if ($balance <= 0) {
            continue; // Skip fully repaid loans
        }

        // Generate bill number
        $loanNoFormatted = $loan->loan_no >= 0
            ? str_pad($loan->loan_no, 6, '0', STR_PAD_LEFT)
            : str_pad($loan->loan_no, 6, '9', STR_PAD_LEFT);

        $billNo = $branchCode . $schemeCode . $loanNoFormatted;

        $csvRows[] = [
            $schemeCode,
            $branchCode,
            $group->mis_kyc_number,
            $billNo,
            $loan->loan_date,
            number_format($loan->loan_amount, 2, '.', ''),
            number_format($balance, 2, '.', ''),
            $payment->due_date,
            $payment->int_rate,
            $group->name,
            $lastInterestPaid,
            '', // total_installments
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No GD loan outstanding data found for export.',
        ]);
    }

    $filename = 'gd_loan_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->json([
            'status' => true,
            'file_url' => asset("storage/" . $filename),
            'file_name' => $filename,
            'message' => 'Gold loan outstanding exported successfully',
        ]);
}
public function downloadDepositLoanOutstandingCsv(Request $request)
{
    $request->validate([
        'data_extraction_id' => 'required|integer',
        'software'           => 'required|string',
        'branch_code'        => 'required|string',
        'scheme_code'        => 'required|string',
    ]);

    $extractionId = $request->input('data_extraction_id');
    $software     = strtolower($request->input('software'));
    $branchCode   = $request->input('branch_code');
    $schemeCode   = $request->input('scheme_code');

    $prefix = $software . '_';

    $dlSchemeMasterTable = $prefix . 'dl_scheme_master';
    $fdHdTable           = $prefix . 'fd_hd';
    $ddOpeningTable      = $prefix . 'dd_opening';
    $dlSecurityTable     = $prefix . 'dl_security';
    $dlOpeningTable      = $prefix . 'dl_opening';
    $dlRepayTable        = $prefix . 'dl_repay';

    $requiredTables = [
        $dlSchemeMasterTable,
        $fdHdTable,
        $ddOpeningTable,
        $dlSecurityTable,
        $dlOpeningTable,
        $dlRepayTable,
    ];

    foreach ($requiredTables as $table) {
        if (!Schema::hasTable($table)) {
            return response()->json([
                'status' => false,
                'message' => "Required table missing: $table",
            ], 400);
        }
    }

    // Get all dl_security loan entries for this extraction id
    $loanEntries = DB::table($dlSecurityTable)
        ->where('data_extraction_id', $extractionId)
        ->get();

    if ($loanEntries->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No deposit loan entries found.',
        ]);
    }

    $loanNos = $loanEntries->pluck('loan_no')->toArray();

    // Fetch loan opening info (due_date, int_rate, closed)
    $loanOpenings = DB::table($dlOpeningTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->where('closed', 'N')
        ->get()
        ->keyBy('loan_no');

    if ($loanOpenings->isEmpty()) {
        return response()->json([
            'status' => false,
            'message' => 'No active loan openings found.',
        ]);
    }

    // Get repayments sum(principal), max(int_upto) grouped by loan_no
    $repayments = DB::table($dlRepayTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn('loan_no', $loanNos)
        ->select('loan_no', DB::raw('SUM(principal) as total_principal'), DB::raw('MAX(int_upto) as last_interest_paid'))
        ->groupBy('loan_no')
        ->get()
        ->keyBy('loan_no');

    // Compose composite keys for deposit accounts (agent_code + account_no)
    $accountKeys = $loanEntries->map(function($loan) {
        return ($loan->dep_agent_code ?: 'NULL') . '__' . $loan->dep_ac_no;
    })->unique()->toArray();

    // Fetch fd_hd keyed by agent_code__fd_no
    $fdRecords = DB::table($fdHdTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn(DB::raw("CONCAT(agent_code, '__', fd_no)"), $accountKeys)
        ->get()
        ->keyBy(function($item) {
            return ($item->agent_code ?: 'NULL') . '__' . $item->fd_no;
        });

    // Fetch dd_opening keyed by agent_code__ac_no
    $ddRecords = DB::table($ddOpeningTable)
        ->where('data_extraction_id', $extractionId)
        ->whereIn(DB::raw("CONCAT(agent_code, '__', ac_no)"), $accountKeys)
        ->get()
        ->keyBy(function($item) {
            return ($item->agent_code ?: 'NULL') . '__' . $item->ac_no;
        });

    $csvHeader = [
        'scheme code',
        'branch',
        'kyc number',
        'loan number',
        'opened date',
        'loan amount',
        'loan balance',
        'due date',
        'interest rate',
        'name',
        'last interest paid date',
        'total installments',
    ];

    $csvRows = [];
    $csvRows[] = $csvHeader;

    foreach ($loanEntries as $loan) {
        $loanNo = $loan->loan_no;

        // Must have an active loan opening
        if (!isset($loanOpenings[$loanNo])) {
            continue;
        }
        $opening = $loanOpenings[$loanNo];

        $repay = $repayments[$loanNo] ?? null;

        $totalPrincipalPaid = $repay->total_principal ?? 0;
        $lastInterestPaid = $repay->last_interest_paid ?? '';

        $balance = $loan->loan_amount - $totalPrincipalPaid;

        if ($balance <= 0) {
            continue; // Skip fully repaid loans
        }

        $agentCode = $loan->dep_agent_code ?: 'NULL';
        $accountKey = $agentCode . '__' . $loan->dep_ac_no;

        if (!empty($loan->dep_agent_code)) {
            $ddRecord = $ddRecords[$accountKey] ?? null;
            $kycNumber = $ddRecord->mis_kyc_number ?? '';
            $name = $ddRecord->name ?? '';
        } else {
            $fdRecord = $fdRecords[$accountKey] ?? null;
            $kycNumber = $fdRecord->mis_kyc_number ?? '';
            $name = $fdRecord->name ?? '';
        }

        // Generate loan number bill_no = branch_code + scheme_code + loan_no formatted
        $loanNoFormatted = $loanNo >= 0
            ? str_pad($loanNo, 6, '0', STR_PAD_LEFT)
            : str_pad($loanNo, 6, '9', STR_PAD_LEFT);

        $billNo = $branchCode . $schemeCode . $loanNoFormatted;

        $csvRows[] = [
            $schemeCode,
            $branchCode,
            $kycNumber,
            $billNo,
            $loan->loan_date,
            number_format($loan->loan_amount, 2, '.', ''),
            number_format($balance, 2, '.', ''),
            $opening->due_date,
            $opening->int_rate,
            $name,
            $lastInterestPaid,
            '', // total_installments blank
        ];
    }

    if (count($csvRows) === 1) {
        return response()->json([
            'status' => false,
            'message' => 'No deposit loan outstanding data found for export.',
        ]);
    }

    $filename = 'deposit_loan_outstanding_' . now()->format('Ymd_His') . '.csv';
    $filepath = storage_path("app/public/$filename");

    $handle = fopen($filepath, 'w');
    foreach ($csvRows as $row) {
        fputcsv($handle, $row);
    }
    fclose($handle);

    return response()->json([
        'status' => true,
        'file_url' => asset("storage/" . $filename),
        'file_name' => $filename,
        'message' => 'Deposit loan outstanding exported successfully',
    ]);
}


}
