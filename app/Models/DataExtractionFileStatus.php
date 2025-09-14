<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class DataExtractionFileStatus extends Model
{
    use HasFactory;
    protected $table = 'data_extraction_file_status';
    protected $guarded = [];
}
