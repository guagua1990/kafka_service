class CreateJobStats < ActiveRecord::Migration
  def up
    create_table :job_stats do |t|
    	t.integer    :job_id,        :limit => 8, :null => false
    	t.integer    :count_success, :limit => 8, :null => false
    	t.integer    :count_failure, :limit => 8, :null => false
      t.timestamps
    end

    add_index :job_stats, :job_id
  end

  def down
  	delete_table :job_stats
  end
end
