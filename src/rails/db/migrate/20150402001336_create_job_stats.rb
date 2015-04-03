class CreateJobStats < ActiveRecord::Migration
  def up
    create_table :job_stats do |t|
    	t.integer    :job_id,               :limit => 8, :null => false
    	t.integer    :count_error,          :limit => 8, :null => true, :default => 0
    	t.integer    :count_actual_total,   :limit => 8, :null => true, :default => 0
    	t.integer    :count_expected_total, :limit => 8, :null => true, :default => 0

      t.timestamps :null => false
    end

    add_index :job_stats, :job_id, :unique => true
  end

  def down
  	drop_table :job_stats
  end
end
