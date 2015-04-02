class AddCountTotalToJobStats < ActiveRecord::Migration
  def up
    add_column :job_stats, :count_total, :integer, :limit => 8, :null => false
  end

  def down
    remove_column :job_stats, :count_total
  end
end
