<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class DataExtraction extends Model
{
    use HasFactory;
    protected $table = 'data_extraction';
    protected $guarded = [];

    public function fileStatus()
    {
        return $this->hasMany(DataExtractionFileStatus::class, 'data_extraction_id');
    }
}
