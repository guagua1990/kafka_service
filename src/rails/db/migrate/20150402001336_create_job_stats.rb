class CreateJobStats < ActiveRecord::Migration
  def up
    create_table :job_stats do |t|
    	t.integer    :job_id,        :limit => 8, :null => false
    	t.integer    :count_success, :limit => 8, :null => true, :default => 0
    	t.integer    :count_failure, :limit => 8, :null => true, :default => 0
    	t.integer    :count_total,   :limit => 8, :null => true, :default => 0

      t.timestamps :null => false

      t.index      :job_id, :unique => true
    end
  end

  def down
  	drop_table :job_stats
  end
end
