<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class DataExtractionTypes extends Model
{
        use HasFactory;
    protected $table = 'data_extraction_types'; // Make sure this matches your actual table name
    protected $primaryKey = 'id'; // Change this if your primary key has a different name
    public $timestamps = false; // Set to false if your table doesn't have created_at/updated_at columns

    public function files()
    {
        return $this->hasMany(DataExtractionTypeFiles::class, 'data_extraction_type_id');
    }

    public function extractions()
    {
        return $this->hasMany(DataExtraction::class, 'data_extraction_type_id');
    }
}

