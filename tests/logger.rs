use fsim::logger;

#[test]
fn test_logger() -> anyhow::Result<()> {
    logger::LoggerBuilder::default()
        .level(log::LevelFilter::Trace)
        .init()?;
    log::info!("Info");
    log::debug!("Debug");
    log::warn!("Warn");
    log::error!("Error");
    let logs = logger::clear()?;
    assert_eq!(
        logs,
        vec![
            String::from("[INFO]  Info"),
            String::from("[DEBUG] Debug"),
            String::from("[WARN]  Warn"),
            String::from("[ERROR] Error"),
        ]
    );
    log::error!("Error");
    log::warn!("Warn");
    log::debug!("Debug");
    log::info!("Info");
    let logs = logger::clear()?;
    assert_eq!(
        logs,
        vec![
            String::from("[ERROR] Error"),
            String::from("[WARN]  Warn"),
            String::from("[DEBUG] Debug"),
            String::from("[INFO]  Info"),
        ]
    );
    Ok(())
}
