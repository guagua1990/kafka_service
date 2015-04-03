class UpdateIndex < ActiveRecord::Migration
  def up
    remove_index :job_stats, :name => "index_job_stats_on_job_id"
    add_index    :job_stats, [:job_id, :irc_id, :field_id], :name => "job_stats_index", :unique => true
  end

  def down
    add_index    :job_stats, :job_id, :name => "index_job_stats_on_job_id"
    remove_index :job_stats, :name => "job_stats_index"
  end
end
