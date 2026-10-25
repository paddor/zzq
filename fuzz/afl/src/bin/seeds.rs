//! Writes the seeds of every target to `seeds/<target>/`.

fn main() -> std::io::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds");
    for (name, _) in zzq_fuzz::TARGETS {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir)?;
        for (i, seed) in zzq_fuzz::seeds::for_target(name).iter().enumerate() {
            std::fs::write(dir.join(format!("{i:02}")), seed)?;
        }
    }
    println!("{}", root.display());
    Ok(())
}
